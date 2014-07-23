using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.IO;
using System.Diagnostics;

namespace Bcp
{
    public abstract class BcpSession
    {

        private static bool between(uint low, uint high, uint test)
        {
            if (low < high)
            {
                return test >= low && test < high;
            }
            else if (low > high)
            {
                return test >= low || test < high;
            }
            else
            {
                return false;
            }
        }

        private sealed class IDSetIsFullException : Exception { }

        internal sealed class IDSet : HashSet<uint>
        {

            private const uint MaxUnconfirmedIds = 1024;

            private uint lowID;

            private uint highID;

            private void compat()
            {
                while (base.Contains(lowID))
                {
                    base.Remove(lowID);
                    lowID += 1;
                }
            }

            public new void Add(uint id)
            {
                if (between(lowID, highID, id))
                {
                    base.Add(id);
                    compat();
                }
                else if (between(lowID, lowID + MaxUnconfirmedIds, id))
                {
                    highID += 1;
                    base.Add(id);
                    compat();
                }
            }

            public bool IsReceived(uint id)
            {
                if (between(lowID, highID, id))
                {
                    return base.Contains(id);
                }
                else if (between(highID, highID + MaxUnconfirmedIds, id))
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }

            public bool AllReceivedBelow(uint id)
            {
                return (!(this.Any<uint>())) && lowID == id && highID == id;
            }

        }

        internal class Connection
        {
            internal Stream stream;
            internal uint FinishID;
            internal bool IsFinishIDReceived = false;
            internal bool IsFinishSent = false;
            internal bool IsShutedDown = false;
            internal uint NumDataReceived = 0;
            internal IDSet ReceiveIDSet = new IDSet();
            internal uint NumDataSent = 0;
            internal uint NumAcknowledgeReceivedForData = 0;
            internal Queue<Bcp.IAcknowledgeRequired> UnconfirmedPackets = new Queue<Bcp.IAcknowledgeRequired>();
            internal Timer HeartBeatTimer;
        }

        protected Object sessionLock = new Object();

        private long allConfirmed = long.MaxValue;

        internal abstract Connection newConnection();

        private static SortedDictionary<long, HashSet<Connection>> newSendingConnectionQueue()
        {
            return new SortedDictionary<long, HashSet<Connection>>(new BcpUtil.DescendingComparer<long>());
        }

        protected abstract void unavailable();

        protected abstract void available();

        protected abstract void shutedDown();

        protected abstract void interrupted();

        internal abstract void release();

        protected abstract void received(IList<ArraySegment<Byte>> buffers);

        private uint lastConnectionId = 0;

        internal Dictionary<uint, Connection> connections = new Dictionary<uint, Connection>();

        private enum SessionState { Available, Unavailable }

        private SessionState sessionState = SessionState.Unavailable;

        private SortedDictionary<long, HashSet<Connection>> sendingConnectionQueue = newSendingConnectionQueue();

        private Queue<Bcp.IAcknowledgeRequired> packQueue = new Queue<Bcp.IAcknowledgeRequired>();

        internal abstract void busy(Connection connection);

        internal abstract void idle(Connection connection);

        internal abstract void close(Connection connection);

        private void addOpenConnection(Connection connection)
        {
            HashSet<Connection> openConnections;
            switch (sessionState)
            {
                case SessionState.Available:
                    if (sendingConnectionQueue.TryGetValue(allConfirmed, out openConnections))
                    {
                        openConnections.Add(connection);
                    }
                    else
                    {
                        openConnections = new HashSet<Connection>();
                        openConnections.Add(connection);
                        sendingConnectionQueue.Add(allConfirmed, openConnections);
                    }
                    break;
                case SessionState.Unavailable:
                    Debug.WriteLine("Unavailable add open connection, packQueue count: " + packQueue.Count());
                    Stream stream = connection.stream;
                    while (packQueue.Count > 0)
                    {
                        var pack = packQueue.Dequeue();
                        BcpIO.Write(stream, pack);
                        connection.UnconfirmedPackets.Enqueue(pack);
                    }
                    stream.Flush();
                    available();
                    openConnections = new HashSet<Connection>();
                    openConnections.Add(connection);
                    sendingConnectionQueue = newSendingConnectionQueue();
                    if (packQueue.Count == 0)
                    {
                        sendingConnectionQueue.Add(allConfirmed, openConnections);
                    }
                    else
                    {
                        sendingConnectionQueue.Add(Environment.TickCount, openConnections);
                        busy(connection);
                    }
                    sessionState = SessionState.Available;
                    Debug.WriteLine("After unavailable add open connection, sendingConnectionQueue count: " + sendingConnectionQueue.Count());
                    break;
            }
        }

        private void removeOpenConnection(Connection connection)
        {
            Debug.WriteLine("Remove open connection!");
            switch (sessionState)
            {
                case SessionState.Available:
                    {
                        foreach (var connections in sendingConnectionQueue)
                        {
                            if (connections.Value.Contains(connection))
                            {
                                if (connections.Value.Count == 1)
                                {
                                    sendingConnectionQueue.Remove(connections.Key);
                                    if (sendingConnectionQueue.Count == 0)
                                    {
                                        sessionState = SessionState.Unavailable;
                                        unavailable();
                                    }
                                }
                                else
                                {
                                    connections.Value.Remove(connection);
                                }
                                break;
                            }
                        }
                        break;
                    }
                case SessionState.Unavailable:
                    break;
            }
            Debug.WriteLine("After remove open connetion, sendConnectionQueue count: " + sendingConnectionQueue.Count());
        }

        private void trySend(Bcp.IPacket newPack)
        {
            switch (sessionState)
            {
                case SessionState.Available:
                    {
                        long time = sendingConnectionQueue.First().Key;
                        HashSet<Connection> openConnections = sendingConnectionQueue.First().Value;
                        Connection connection = openConnections.First();
                        Stream stream = connection.stream;
                        BcpIO.Write(stream, newPack);
                        stream.Flush();
                        resetHeartBeatTimer(connection);
                        openConnections.Remove(connection);
                        long currentTimeMillis = Environment.TickCount;
                        HashSet<Connection> currentOpenConnections;
                        if (sendingConnectionQueue.TryGetValue(currentTimeMillis, out currentOpenConnections))
                        {
                            if (openConnections.Count == 0)
                            {
                                sendingConnectionQueue.Remove(time);
                            }
                            currentOpenConnections.Add(connection);
                        }
                        else
                        {
                            currentOpenConnections = new HashSet<Connection>();
                            currentOpenConnections.Add(connection);
                            sendingConnectionQueue.Add(currentTimeMillis, currentOpenConnections);
                        }
                        break;
                    }
                case SessionState.Unavailable:
                    break;
            }
        }

        private void enqueue(Bcp.IAcknowledgeRequired newPack)
        {
            Debug.WriteLine("Enqueue pack: " + newPack);
            switch (sessionState)
            {
                case SessionState.Available:
                    {
                        Debug.WriteLine("Before available enqueue sendingConnectionQueue count: " + sendingConnectionQueue.Count());
                        long time = sendingConnectionQueue.First().Key;
                        HashSet<Connection> openConnections = sendingConnectionQueue.First().Value;
                        Connection connection = openConnections.First();
                        Stream stream = connection.stream;
                        busy(connection);
                        BcpIO.Write(stream, newPack);
                        stream.Flush();
                        connection.UnconfirmedPackets.Enqueue(newPack);
                        resetHeartBeatTimer(connection);
                        long currentTimeMillis = Environment.TickCount;
                        HashSet<Connection> currentOpenConnections;
                        openConnections.Remove(connection);
                        if (openConnections.Count == 0)
                        {
                            sendingConnectionQueue.Remove(time);
                        }
                        if (sendingConnectionQueue.TryGetValue(currentTimeMillis, out currentOpenConnections))
                        {
                            currentOpenConnections.Add(connection);
                        }
                        else
                        {
                            currentOpenConnections = new HashSet<Connection>();
                            currentOpenConnections.Add(connection);
                            sendingConnectionQueue.Add(currentTimeMillis, currentOpenConnections);
                        }
                        Debug.WriteLine("After available enqueue sendingConnectionQueue count: " + sendingConnectionQueue.Count());
                        break;
                    }
                case SessionState.Unavailable:
                    {
                        Debug.WriteLine("Before Unavailable enqueue: " + packQueue.Count());
                        if (packQueue.Count() >= Bcp.MaxOfflinePack)
                        {
                            throw new BcpException.SendingQueueIsFull();
                        }
                        else
                        {
                            packQueue.Enqueue(newPack);
                        }
                        Debug.WriteLine("After Unavailable enqueue: " + packQueue.Count());
                    }
                    break;
            }
        }

        private void enqueueFinish(Connection connection)
        {
            if (!connection.IsFinishSent)
            {
                Bcp.Finish finishPacket = new Bcp.Finish();
                connection.UnconfirmedPackets.Enqueue(finishPacket);
                Stream stream = connection.stream;
                BcpIO.Write(stream, finishPacket);
                stream.Flush();
                connection.IsFinishSent = true;
            }
        }

        internal void finishConnection(uint connectionId, Connection connection)
        {
            enqueueFinish(connection);
            removeOpenConnection(connection);
            checkConnectionFinish(connectionId, connection);
        }

        private void checkConnectionFinish(uint connectionId, Connection connection)
        {
            bool isConnectionFinish =
                connection.IsFinishSent &&
                connection.IsFinishIDReceived &&
                connection.ReceiveIDSet.AllReceivedBelow(connection.FinishID) &&
                connection.UnconfirmedPackets.Count == 0;
            if (isConnectionFinish)
            {
                connections.Remove(connectionId);
            }
        }

        private void dataReceived(uint connectionId, Connection connection, uint packId, IList<ArraySegment<Byte>> buffer)
        {
            var idSet = connection.ReceiveIDSet;
            if (idSet.IsReceived(packId))
            {
            }
            else
            {
                received(buffer);
                connection.ReceiveIDSet.Add(packId);
                checkConnectionFinish(connectionId, connection);
            }
        }

        private void checkShutDown()
        {
            Debug.WriteLine("Check shut down!");
            trySend(new Bcp.ShutDown());
            release();
            switch (sessionState)
            {
                case SessionState.Available:
                    {
                        foreach (var openConnections in sendingConnectionQueue.Values)
                        {
                            foreach (var connection in openConnections)
                            {
                                connection.IsShutedDown = true;
                                close(connection);
                                var stream = connection.stream;
                                var heartBeatTimer = connection.HeartBeatTimer;
                                heartBeatTimer.Dispose();
                                connection.HeartBeatTimer = null;
                                connection.stream = null;
                                stream.Dispose();
                            }
                        }
                        sessionState = SessionState.Unavailable;
                        packQueue = new Queue<Bcp.IAcknowledgeRequired>();
                        break;
                    }
                case SessionState.Unavailable:
                    break;
            }
            shutedDown();
        }

        private void retransmissionFinishReceived(uint connectionId, Connection connection, uint packId)
        {
            connection.NumDataReceived = packId + 1;
            if (!connection.IsFinishIDReceived)
            {
                connection.FinishID = packId;
                checkConnectionFinish(connectionId, connection);
            }
            else
            {
                throw new BcpException.AlreadyReceivedFinish();
            }
        }

        private void cleanUp(uint connectionId, Connection connection)
        {
            Debug.WriteLine("Cleanning up connectionId: " + connectionId);
            removeOpenConnection(connection);
            if (!connection.IsFinishSent)
            {
                var finishPacket = new Bcp.Finish();
                connection.UnconfirmedPackets.Enqueue(finishPacket);
                connection.IsFinishSent = true;
            }
            close(connection);
            var stream = connection.stream;
            connection.stream = null;
            if (stream != null)
            {
                var heartBeatTimer = connection.HeartBeatTimer;
                connection.HeartBeatTimer = null;
                if (heartBeatTimer != null)
                {
                    heartBeatTimer.Dispose();
                }
            }
            var packId = connection.NumAcknowledgeReceivedForData;
            foreach (Bcp.IAcknowledgeRequired packet in connection.UnconfirmedPackets)
            {
                if (packet is Bcp.Data)
                {
                    var oldPacket = (Bcp.Data)packet;
                    var newPacket = new Bcp.RetransmissionData(connectionId, packId, oldPacket.Buffers);
                    enqueue(newPacket);
                    packId += 1;
                }
                else if (packet is Bcp.Finish)
                {
                    var newPacket = new Bcp.RetransmissionFinish(connectionId, packId);
                    enqueue(newPacket);
                    packId += 1;
                }
                else
                {
                    enqueue(packet);
                }
            }
            connection.UnconfirmedPackets = new Queue<Bcp.IAcknowledgeRequired>();
            checkConnectionFinish(connectionId, connection);
        }

        private void startReceive(uint connectionId, Connection connection)
        {
            BcpDelegate.ProcessRead processRead = delegate(Bcp.IPacket packet)
            {
                if (packet is Bcp.HeartBeat)
                {
                    Debug.WriteLine("Receive heart beat!");
                    startReceive(connectionId, connection);
                }
                else if (packet is Bcp.Data)
                {
                    Debug.WriteLine("Receive data: " + packet);
                    lock (sessionLock)
                    {
                        BcpIO.Write(connection.stream, new Bcp.Acknowledge());
                        var data = (Bcp.Data)packet;
                        var buffer = data.Buffers;
                        resetHeartBeatTimer(connection);
                        var packId = connection.NumDataReceived;
                        connection.NumDataReceived = packId + 1;
                        dataReceived(connectionId, connection, packId, buffer);
                    }
                    startReceive(connectionId, connection);
                    connection.stream.Flush();
                }
                else if (packet is Bcp.RetransmissionData)
                {
                    Debug.WriteLine("Receive retransmission data: " + packet);
                    lock (sessionLock)
                    {
                        BcpIO.Write(connection.stream, new Bcp.Acknowledge());
                        var retransmissionData = (Bcp.RetransmissionData)packet;
                        var dataConnectionId = retransmissionData.ConnectionId;
                        var packId = retransmissionData.PackId;
                        var data = retransmissionData.Buffers;

                        resetHeartBeatTimer(connection);
                        Connection dataConnection;
                        if (connections.TryGetValue(dataConnectionId, out dataConnection))
                        {
                            dataReceived(dataConnectionId, dataConnection, packId, data);
                        }
                        else
                        {
                            var oldLastConnectionId = lastConnectionId;
                            if (dataConnectionId - oldLastConnectionId + connections.Count() >= Bcp.MaxConnectionsPerSession)
                            {
                                internalInterrupt();
                            }
                            else
                            {
                                if (oldLastConnectionId < dataConnectionId)
                                {
                                    lastConnectionId = dataConnectionId;
                                    for (var id = oldLastConnectionId + 1; id <= dataConnectionId; ++id)
                                    {
                                        Connection c = newConnection();
                                        connections.Add(id, c);
                                    }
                                    dataReceived(dataConnectionId, connections.Last().Value, packId, data);
                                }
                                else
                                {
                                }
                            }
                        }
                    }
                    startReceive(connectionId, connection);
                    connection.stream.Flush();
                }
                else if (packet is Bcp.Acknowledge)
                {
                    Debug.WriteLine("Receive acknowledge: " + packet);
                    lock (sessionLock)
                    {
                        var originalPack = connection.UnconfirmedPackets.Dequeue();
                        if (connection.UnconfirmedPackets.Count() == 0)
                        {
                            switch (sessionState)
                            {
                                case SessionState.Available:
                                    {
                                        foreach (var openConnections in sendingConnectionQueue)
                                        {
                                            if (openConnections.Value.Contains(connection))
                                            {
                                                openConnections.Value.Remove(connection);
                                                if (openConnections.Value.Count() == 0)
                                                {
                                                    sendingConnectionQueue.Remove(openConnections.Key);
                                                }
                                                HashSet<Connection> allConfirmedConnections;
                                                if (sendingConnectionQueue.TryGetValue(allConfirmed, out allConfirmedConnections))
                                                {
                                                    allConfirmedConnections.Add(connection);
                                                }
                                                else
                                                {
                                                    allConfirmedConnections = new HashSet<Connection>();
                                                    allConfirmedConnections.Add(connection);
                                                    sendingConnectionQueue.Add(allConfirmed, allConfirmedConnections);
                                                }
                                                break;
                                            }
                                        }
                                        break;
                                    }
                                case SessionState.Unavailable:
                                    break;
                            }
                            idle(connection);
                        }
                        if (originalPack is Bcp.Data)
                        {
                            connection.NumAcknowledgeReceivedForData += 1;
                        }
                        else if (originalPack is Bcp.RetransmissionData ||
                            originalPack is Bcp.Finish || originalPack is Bcp.RetransmissionFinish)
                        {
                        }
                        checkConnectionFinish(connectionId, connection);
                    }
                    startReceive(connectionId, connection);
                }
                else if (packet is Bcp.Finish)
                {
                    Debug.WriteLine("receive finish, connectionId: ", connectionId);
                    lock (sessionLock)
                    {
                        BcpIO.Write(connection.stream, new Bcp.Acknowledge());
                        if (!connection.IsFinishSent)
                        {
                            enqueueFinish(connection);
                        }
                        var packId = connection.NumDataReceived;
                        connection.FinishID = packId;
                        cleanUp(connectionId, connection);
                    }
                    connection.stream.Dispose();
                    connection.stream = null;
                }
                else if (packet is Bcp.RetransmissionFinish)
                {
                    Debug.WriteLine("Receive retransmission finish: " + packet);
                    lock (sessionLock)
                    {
                        BcpIO.Write(connection.stream, new Bcp.Acknowledge());
                        var retransmissionFinishPack = (Bcp.RetransmissionFinish)packet;
                        var finishConnectionId = retransmissionFinishPack.ConnectionId;
                        var packId = retransmissionFinishPack.PackId;
                        resetHeartBeatTimer(connection);
                        Connection finishConnection;
                        if (connections.TryGetValue(finishConnectionId, out finishConnection))
                        {
                            retransmissionFinishReceived(finishConnectionId, finishConnection, packId);
                            cleanUp(finishConnectionId, finishConnection);
                        }
                        else
                        {
                            var oldLastConnectionId = lastConnectionId;
                            if (finishConnectionId - oldLastConnectionId + connections.Count() >= Bcp.MaxConnectionsPerSession)
                            {
                                internalInterrupt();
                            }
                            else
                            {
                                if (oldLastConnectionId < finishConnectionId)
                                {
                                    lastConnectionId = finishConnectionId;
                                    for (var id = oldLastConnectionId + 1; id <= finishConnectionId; ++id)
                                    {
                                        Connection c = newConnection();
                                        connections.Add(id, c);
                                    }
                                    retransmissionFinishReceived(finishConnectionId, connections.Last().Value, packId);
                                    cleanUp(finishConnectionId, connections.Last().Value);
                                }
                                else
                                {
                                }

                            }
                        }
                    }
                    startReceive(connectionId, connection);
                    connection.stream.Flush();
                }
                else if (packet is Bcp.ShutDown)
                {
                    Debug.WriteLine("Receive shut down!");
                    lock (sessionLock)
                    {
                        checkShutDown();
                    }
                }
                else if (packet is Bcp.Renew)
                {
                    lock (sessionLock)
                    {
                        switch (sessionState)
                        {
                            case SessionState.Available:
                                {
                                    foreach (var openConnections in sendingConnectionQueue.Values)
                                    {
                                        foreach (var originalConnection in openConnections)
                                        {
                                            if (originalConnection != connection)
                                            {
                                                originalConnection.stream.Dispose();
                                                originalConnection.stream = null;
                                                originalConnection.HeartBeatTimer.Dispose();
                                                originalConnection.HeartBeatTimer = null;
                                            }
                                        }
                                    }
                                    break;
                                }
                            case SessionState.Unavailable:
                                break;
                        }
                        sessionState = SessionState.Available;
                        connections.Clear();
                        connections.Add(connectionId, connection);
                    }
                }
            };
            BcpDelegate.ExceptionHandler exceptionHandler = delegate(Exception e)
            {
                Debug.WriteLine("Received exception: " + e.Message);
                lock (sessionLock)
                {
                    if (connection.stream != null)
                    {
                        connection.stream.Dispose();
                    }
                    cleanUp(connectionId, connection);
                }
            };
            BcpIO.Read(connection.stream, processRead, exceptionHandler);
        }

        private void resetHeartBeatTimer(Connection connection)
        {
            if (!connection.IsShutedDown)
            {
                var oldTimer = connection.HeartBeatTimer;
                oldTimer.Dispose();
                oldTimer = null;
                var newHeartBeatTimer = new Timer(heartBeatEvent, connection, 0, Bcp.HeartBeatDelayMilliseconds);
                connection.HeartBeatTimer = newHeartBeatTimer;
            }
        }

        private void heartBeatEvent(Object source)
        {
            Debug.WriteLine("Sending heart beat!");
            var connection = (Connection)source;
            lock (sessionLock)
            {
                if (connection.stream != null)
                {
                    BcpIO.Write(connection.stream, new Bcp.HeartBeat());
                    try
                    {
                        connection.stream.Flush();
                    }
                    catch
                    {
                    }
                }
            }
        }

        internal void internalInterrupt()
        {
            foreach (var connection in connections.Values)
            {
                connection.IsShutedDown = true;
                if (connection.stream != null)
                {
                    var oldHeartBeatTimer = connection.HeartBeatTimer;
                    oldHeartBeatTimer.Dispose();
                    connection.HeartBeatTimer = null;
                    connection.stream.Dispose();
                    connection.stream = null;
                }
                connection.UnconfirmedPackets = new Queue<Bcp.IAcknowledgeRequired>();
            }
            sessionState = SessionState.Unavailable;
            packQueue = new Queue<Bcp.IAcknowledgeRequired>();
            connections.Clear();
            interrupted();
        }

        public void Interrupt()
        {
            lock (sessionLock)
            {
                internalInterrupt();
            }
        }

        public void ShutDown()
        {
            lock (sessionLock)
            {
                checkShutDown();
            }
        }

        public void Send(IList<ArraySegment<Byte>> buffer)
        {
            lock (sessionLock)
            {
                enqueue(new Bcp.Data(buffer));
            }
        }

        internal void addStream(uint connectionId, Stream stream)
        {
            lock (sessionLock)
            {
                uint oldLastConnectionId = lastConnectionId;
                if (connections.Count >= Bcp.MaxConnectionsPerSession ||
                    activeConnectionNum() >= Bcp.MaxActiveConnectionsPerSession)
                {
                    stream.Dispose();
                }
                if (connectionId < oldLastConnectionId ||
                    connectionId - oldLastConnectionId + connections.Count >= Bcp.MaxConnectionsPerSession)
                {
                    internalInterrupt();
                }
                else
                {
                    if (connectionId > oldLastConnectionId + 1)
                    {
                        for (uint id = oldLastConnectionId + 1; id < connectionId; ++id)
                        {
                            if (!connections.ContainsKey(id))
                            {
                                Connection c = newConnection();
                                connections.Add(id, c);
                            }
                        }
                    }
                    Connection connection;
                    if (connections.TryGetValue(connectionId, out connection))
                    {
                    }
                    else
                    {
                        connection = newConnection();
                    }
                    lastConnectionId = connectionId;
                    if (connection.stream == null)
                    {
                        connection.stream = stream;
                        addOpenConnection(connection);
                        var newHeartBeatTimer = new Timer(heartBeatEvent, connection, 0, Bcp.HeartBeatDelayMilliseconds);
                        connection.HeartBeatTimer = newHeartBeatTimer;
                        startReceive(connectionId, connection);
                    }
                    else
                    {
                        stream.Dispose();
                    }
                }
            }
            Debug.WriteLine("After add stream sendingQueue count: " + sendingConnectionQueue.Count());
        }

        internal int activeConnectionNum()
        {
            int activeConnectionNum = 0;
            foreach (var dictionaryConnection in connections)
            {
                if (dictionaryConnection.Value.stream != null)
                {
                    activeConnectionNum += 1;
                }
            }
            return activeConnectionNum;
        }

    }
}

