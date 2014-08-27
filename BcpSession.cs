/*
 * csharp-bcp
 * Copyright 2014 深圳岂凡网络有限公司 (Shenzhen QiFun Network Corp., LTD)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.IO;
using System.Diagnostics;
using System.Text;

namespace Bcp
{
    public abstract class BcpSession
    {

        private static bool Between(uint low, uint high, uint test)
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

            private void Compat()
            {
                while (base.Contains(lowID))
                {
                    base.Remove(lowID);
                    lowID += 1;
                }
            }

            public new void Add(uint id)
            {
                if (Between(lowID, highID, id))
                {
                    base.Add(id);
                    Compat();
                }
                else if (Between(lowID, lowID + MaxUnconfirmedIds, id))
                {
                    highID += 1;
                    base.Add(id);
                    Compat();
                }
            }

            public bool IsReceived(uint id)
            {
                if (Between(lowID, highID, id))
                {
                    return base.Contains(id);
                }
                else if (Between(highID, lowID + MaxUnconfirmedIds, id))
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
            internal Bcp.ReadState readState = new Bcp.ReadState();
            internal uint finishID;
            internal bool isFinishIDReceived = false;
            internal bool isFinishSent = false;
            internal bool isShutedDown = false;
            internal uint numDataReceived = 0;
            internal IDSet receiveIDSet = new IDSet();
            internal uint numDataSent = 0;
            internal uint numAcknowledgeReceivedForData = 0;
            internal Queue<Bcp.IAcknowledgeRequired> unconfirmedPackets = new Queue<Bcp.IAcknowledgeRequired>();
            internal Timer heartBeatTimer;
        }

        internal Object sessionLock = new Object();

        private long allConfirmed = long.MaxValue;

        internal abstract Connection NewConnection();

        private static SortedDictionary<long, HashSet<Connection>> NewSendingConnectionQueue()
        {
            return new SortedDictionary<long, HashSet<Connection>>(new BcpUtil.DescendingComparer<long>());
        }

        public event EventHandler Unavailable;

        public event EventHandler Available;

        public event EventHandler ShutedDown; 

        public event EventHandler Interrupted;

        public event EventHandler<ReceivedEventArgs> Received;

        public class ReceivedEventArgs : EventArgs
        {
            public ReceivedEventArgs(IList<ArraySegment<Byte>> buffers)
            {
                this.buffers = buffers;
            }
            private IList<ArraySegment<Byte>> buffers;

            public IList<ArraySegment<Byte>> Buffers { get {return buffers; } }
        }

        internal abstract void Release();

        internal uint lastConnectionId = 0;

        internal Dictionary<uint, Connection> connections = new Dictionary<uint, Connection>();

        private enum SessionState { Available, Unavailable }

        private SessionState connectionState = SessionState.Unavailable;

        private SortedDictionary<long, HashSet<Connection>> sendingConnectionQueue = NewSendingConnectionQueue();

        private Queue<Bcp.IAcknowledgeRequired> packQueue = new Queue<Bcp.IAcknowledgeRequired>();

        internal abstract void Busy(Connection connection);

        internal abstract void Idle(Connection connection);

        internal abstract void Close(Connection connection);

        private void AddOpenConnection(Connection connection)
        {
            HashSet<Connection> openConnections;
            switch (connectionState)
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
                        connection.unconfirmedPackets.Enqueue(pack);
                    }
                    stream.Flush();
                    EventHandler availableEventHandler = Available;
                    if (availableEventHandler != null)
                    {
                        availableEventHandler(this, EventArgs.Empty);
                    }
                    openConnections = new HashSet<Connection>();
                    openConnections.Add(connection);
                    sendingConnectionQueue = NewSendingConnectionQueue();
                    if (packQueue.Count == 0)
                    {
                        sendingConnectionQueue.Add(allConfirmed, openConnections);
                    }
                    else
                    {
                        sendingConnectionQueue.Add(Environment.TickCount, openConnections);
                        Busy(connection);
                    }
                    connectionState = SessionState.Available;
                    Debug.WriteLine("After unavailable add open connection, sendingConnectionQueue count: " + sendingConnectionQueue.Count());
                    break;
            }
        }

        private void RemoveOpenConnection(Connection connection)
        {
            Debug.WriteLine("Before remove open connection, sendingConnectionQueue: " + sendingConnectionQueue.Count);
            switch (connectionState)
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
                                        connectionState = SessionState.Unavailable;
                                        EventHandler unavailableEventHandler = Unavailable;
                                        if (unavailableEventHandler != null)
                                        {
                                            unavailableEventHandler(this, EventArgs.Empty);
                                        }
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

        private void TrySend(Bcp.IPacket newPack)
        {
            switch (connectionState)
            {
                case SessionState.Available:
                    {
                        long time = sendingConnectionQueue.First().Key;
                        HashSet<Connection> openConnections = sendingConnectionQueue.First().Value;
                        Connection connection = openConnections.First();
                        Stream stream = connection.stream;
                        BcpIO.Write(stream, newPack);
                        stream.Flush();
                        ResetHeartBeatTimer(connection);
                        openConnections.Remove(connection);
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
                        break;
                    }
                case SessionState.Unavailable:
                    break;
            }
        }

        private void Enqueue(Bcp.IAcknowledgeRequired newPack)
        {
            Debug.WriteLine("Enqueue pack: " + newPack);
            switch (connectionState)
            {
                case SessionState.Available:
                    {
                        Debug.WriteLine("Before available enqueue sendingConnectionQueue count: " + sendingConnectionQueue.Count());
                        long time = sendingConnectionQueue.First().Key;
                        HashSet<Connection> openConnections = sendingConnectionQueue.First().Value;
                        Connection connection = openConnections.First();
                        Stream stream = connection.stream;
                        Busy(connection);
                        BcpIO.Write(stream, newPack);
                        stream.Flush();
                        connection.unconfirmedPackets.Enqueue(newPack);
                        ResetHeartBeatTimer(connection);
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

        private void EnqueueFinish(Connection connection)
        {
            if (!connection.isFinishSent)
            {
                Bcp.Finish finishPacket = new Bcp.Finish();
                connection.unconfirmedPackets.Enqueue(finishPacket);
                Stream stream = connection.stream;
                BcpIO.Write(stream, finishPacket);
                stream.Flush();
                connection.isFinishSent = true;
            }
        }

        internal void FinishConnection(uint connectionId, Connection connection)
        {
            EnqueueFinish(connection);
            RemoveOpenConnection(connection);
            CheckConnectionFinish(connectionId, connection);
        }

        private void CheckConnectionFinish(uint connectionId, Connection connection)
        {
            bool isConnectionFinish =
                connection.isFinishSent &&
                connection.isFinishIDReceived &&
                connection.receiveIDSet.AllReceivedBelow(connection.finishID) &&
                connection.unconfirmedPackets.Count == 0;
            if (isConnectionFinish)
            {
                connections.Remove(connectionId);
            }
        }

        private void DataReceived(uint connectionId, Connection connection, uint packId, IList<ArraySegment<Byte>> buffer)
        {
            var idSet = connection.receiveIDSet;
            if (idSet.IsReceived(packId))
            {
            }
            else
            {
                Debug.WriteLine("Received Message: " + BcpUtil.ArraySegmentListToString(buffer));
                EventHandler<ReceivedEventArgs> receivedEventHandler = Received;
                if (receivedEventHandler != null)
                {
                    receivedEventHandler(this, new ReceivedEventArgs(buffer));
                }
                connection.receiveIDSet.Add(packId);
                CheckConnectionFinish(connectionId, connection);
            }
        }

        private void CheckShutDown()
        {
            Debug.WriteLine("Check shut down!");
            TrySend(new Bcp.ShutDown());
            Release();
            switch (connectionState)
            {
                case SessionState.Available:
                    {
                        foreach (var openConnections in sendingConnectionQueue.Values)
                        {
                            foreach (var connection in openConnections)
                            {
                                connection.isShutedDown = true;
                                Close(connection);
                                var stream = connection.stream;
                                var heartBeatTimer = connection.heartBeatTimer;
                                heartBeatTimer.Change(Timeout.Infinite, Timeout.Infinite);
                                heartBeatTimer.Dispose();
                                connection.heartBeatTimer = null;
                                connection.stream = null;
                                stream.Dispose();
                            }
                        }
                        connectionState = SessionState.Unavailable;
                        packQueue = new Queue<Bcp.IAcknowledgeRequired>();
                        break;
                    }
                case SessionState.Unavailable:
                    break;
            }
            EventHandler shutedDownEventHandler = ShutedDown;
            if (shutedDownEventHandler != null)
            {
                shutedDownEventHandler(this, EventArgs.Empty);
            }
        }

        private void RetransmissionFinishReceived(uint connectionId, Connection connection, uint packId)
        {
            connection.numDataReceived = packId + 1;
            if (!connection.isFinishIDReceived)
            {
                connection.isFinishIDReceived = true;
                connection.finishID = packId;
                CheckConnectionFinish(connectionId, connection);
            }
            else
            {
                throw new BcpException.AlreadyReceivedFinish();
            }
        }

        private void CleanUp(uint connectionId, Connection connection)
        {
            Debug.WriteLine("Cleanning up connectionId: " + connectionId);
            RemoveOpenConnection(connection);
            if (!connection.isFinishSent)
            {
                var finishPacket = new Bcp.Finish();
                connection.unconfirmedPackets.Enqueue(finishPacket);
                connection.isFinishSent = true;
            }
            Close(connection);
            var stream = connection.stream;
            connection.stream = null;
            if (stream != null)
            {
                var heartBeatTimer = connection.heartBeatTimer;
                connection.heartBeatTimer = null;
                if (heartBeatTimer != null)
                {
                    heartBeatTimer.Change(Timeout.Infinite, Timeout.Infinite);
                    heartBeatTimer.Dispose();
                    heartBeatTimer = null;
                }
            }
            var packId = connection.numAcknowledgeReceivedForData;
            foreach (Bcp.IAcknowledgeRequired packet in connection.unconfirmedPackets)
            {
                if (packet is Bcp.Data)
                {
                    var oldPacket = (Bcp.Data)packet;
                    var newPacket = new Bcp.RetransmissionData(connectionId, packId, oldPacket.Buffers);
                    Enqueue(newPacket);
                    packId += 1;
                }
                else if (packet is Bcp.Finish)
                {
                    var newPacket = new Bcp.RetransmissionFinish(connectionId, packId);
                    Enqueue(newPacket);
                    packId += 1;
                }
                else
                {
                    Enqueue(packet);
                }
            }
            connection.unconfirmedPackets = new Queue<Bcp.IAcknowledgeRequired>();
            CheckConnectionFinish(connectionId, connection);
        }

        private void StartReceive(uint connectionId, Connection connection)
        {
            BcpDelegate.ProcessRead processRead = delegate(Bcp.IPacket packet)
            {
                var readTimer = connection.readState.readTimeoutTimer;
                readTimer.Change(Timeout.Infinite, Timeout.Infinite);
                readTimer.Dispose();
                readTimer = null;
                if (packet is Bcp.HeartBeat)
                {
                    Debug.WriteLine("Receive heart beat!");
                    StartReceive(connectionId, connection);
                }
                else if (packet is Bcp.Data)
                {
                    Debug.WriteLine("Receive data: " + packet);
                    lock (sessionLock)
                    {
                        BcpIO.Write(connection.stream, new Bcp.Acknowledge());
                        var data = (Bcp.Data)packet;
                        var buffer = data.Buffers;
                        ResetHeartBeatTimer(connection);
                        var packId = connection.numDataReceived;
                        connection.numDataReceived = packId + 1;
                        DataReceived(connectionId, connection, packId, buffer);
                        connection.stream.Flush();
                    }
                    StartReceive(connectionId, connection);

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
                        ResetHeartBeatTimer(connection);
                        Connection dataConnection;
                        if (connections.TryGetValue(dataConnectionId, out dataConnection))
                        {
                            DataReceived(dataConnectionId, dataConnection, packId, data);
                        }
                        else
                        {
                            var oldLastConnectionId = lastConnectionId;
                            if (dataConnectionId - oldLastConnectionId + connections.Count() >= Bcp.MaxConnectionsPerSession)
                            {
                                InternalInterrupt();
                            }
                            else
                            {
                                if (oldLastConnectionId <= dataConnectionId)
                                {
                                    lastConnectionId = dataConnectionId;
                                    for (var id = oldLastConnectionId + 1; id < dataConnectionId; ++id)
                                    {
                                        Connection c = NewConnection();
                                        connections.Add(id, c);
                                    }
                                    DataReceived(dataConnectionId, connections.Last().Value, packId, data);
                                }
                                else
                                {
                                }
                            }
                        }
                        connection.stream.Flush();
                    }
                    StartReceive(connectionId, connection);
                }
                else if (packet is Bcp.Acknowledge)
                {
                    lock (sessionLock)
                    {
                        Debug.WriteLine("Before receive acknowledge, sendingConnectionQueue: " + sendingConnectionQueue.Count);
                        var originalPack = connection.unconfirmedPackets.Dequeue();
                        if (connection.unconfirmedPackets.Count() == 0)
                        {
                            switch (connectionState)
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
                            Idle(connection);
                        }
                        if (originalPack is Bcp.Data)
                        {
                            connection.numAcknowledgeReceivedForData += 1;
                        }
                        else if (originalPack is Bcp.RetransmissionData ||
                            originalPack is Bcp.Finish || originalPack is Bcp.RetransmissionFinish)
                        {
                        }
                        CheckConnectionFinish(connectionId, connection);
                        Debug.WriteLine("After receive acknowledge, sendingConnectionQueue: " + sendingConnectionQueue.Count);
                    }
                    StartReceive(connectionId, connection);
                }
                else if (packet is Bcp.Finish)
                {
                    Debug.WriteLine("receive finish, connectionId: ", connectionId);
                    lock (sessionLock)
                    {
                        BcpIO.Write(connection.stream, new Bcp.Acknowledge());
                        if (!connection.isFinishSent)
                        {
                            EnqueueFinish(connection);
                        }
                        var packId = connection.numDataReceived;
                        connection.finishID = packId;
                        CleanUp(connectionId, connection);
                        connection.stream.Dispose();
                        connection.stream = null;
                    }
                }
                else if (packet is Bcp.RetransmissionFinish)
                {
                    lock (sessionLock)
                    {
                        Debug.WriteLine("Before receive retransmission finish, sendingConnectionQueue: " + sendingConnectionQueue.Count);
                        BcpIO.Write(connection.stream, new Bcp.Acknowledge());
                        var retransmissionFinishPack = (Bcp.RetransmissionFinish)packet;
                        var finishConnectionId = retransmissionFinishPack.ConnectionId;
                        var packId = retransmissionFinishPack.PackId;
                        ResetHeartBeatTimer(connection);
                        Connection finishConnection;
                        if (connections.TryGetValue(finishConnectionId, out finishConnection))
                        {
                            RetransmissionFinishReceived(finishConnectionId, finishConnection, packId);
                            CleanUp(finishConnectionId, finishConnection);
                        }
                        else
                        {
                            var oldLastConnectionId = lastConnectionId;
                            if (finishConnectionId - oldLastConnectionId + connections.Count() >= Bcp.MaxConnectionsPerSession)
                            {
                                InternalInterrupt();
                            }
                            else
                            {
                                if (oldLastConnectionId < finishConnectionId)
                                {
                                    lastConnectionId = finishConnectionId;
                                    for (var id = oldLastConnectionId + 1; id <= finishConnectionId; ++id)
                                    {
                                        Connection c = NewConnection();
                                        connections.Add(id, c);
                                    }
                                    RetransmissionFinishReceived(finishConnectionId, connections.Last().Value, packId);
                                    CleanUp(finishConnectionId, connections.Last().Value);
                                }
                                else
                                {
                                }
                            }
                        }
                        connection.stream.Flush();
                        Debug.WriteLine("After receive retransmission finish, sendingConnectionQueue: " + sendingConnectionQueue.Count);
                    }
                    StartReceive(connectionId, connection);
                }
                else if (packet is Bcp.ShutDown)
                {
                    Debug.WriteLine("Receive shut down!");
                    lock (sessionLock)
                    {
                        CheckShutDown();
                    }
                }
            };
            BcpDelegate.ExceptionHandler exceptionHandler = delegate(Exception e)
            {
                lock (sessionLock)
                {
                    if (!connection.readState.isCancel)
                    {
                        connection.readState.Cancel();
                        Console.WriteLine("Received exception: " + e.Message);
                        if (connection.stream != null)
                        {
                            connection.stream.Dispose();
                        }
                        CleanUp(connectionId, connection);
                    }
                }
            };
            connection.readState.readTimeoutTimer = StartReadTimer(connection.stream, exceptionHandler);
            BcpIO.Read(connection.stream, connection.readState, processRead, exceptionHandler);
        }

        internal Timer StartReadTimer(Stream stream, BcpDelegate.ExceptionHandler exceptionHandler)
        {
            TimerCallback readTimeoutCallback = delegate(Object source)
            {
                stream.Dispose();
                exceptionHandler(new Exception());
            };
            var readTimer = new Timer(readTimeoutCallback, null, Bcp.ReadingTimeoutMilliseconds, Bcp.ReadingTimeoutMilliseconds);
            return readTimer;
        }

        private void ResetHeartBeatTimer(Connection connection)
        {
            if (!connection.isShutedDown)
            {
                var oldTimer = connection.heartBeatTimer;
                oldTimer.Change(Timeout.Infinite, Timeout.Infinite);
                oldTimer.Dispose();
                oldTimer = null;
                var newHeartBeatTimer = new Timer(HeartBeatEvent, connection, Bcp.HeartBeatDelayMilliseconds, Bcp.HeartBeatDelayMilliseconds);
                connection.heartBeatTimer = newHeartBeatTimer;
            }
        }

        internal void RenewSession()
        {
            switch (connectionState)
            {
                case SessionState.Available:
                    {
                        foreach (var openConnections in sendingConnectionQueue.Values)
                        {
                            foreach (var originalConnection in openConnections)
                            {
                                originalConnection.readState.Cancel();
                                originalConnection.stream.Close();
                                originalConnection.stream.Dispose();
                                originalConnection.stream = null;
                                originalConnection.heartBeatTimer.Change(Timeout.Infinite, Timeout.Infinite);
                                originalConnection.heartBeatTimer.Dispose();
                                originalConnection.heartBeatTimer = null;
                            }
                        }
                        break;
                    }
                case SessionState.Unavailable:
                    break;
            }
            lastConnectionId = 0;
            connectionState = SessionState.Unavailable;
            sendingConnectionQueue.Clear();
            connections.Clear();
        }

        private void HeartBeatEvent(Object source)
        {
            Debug.WriteLine("Sending heart beat!");
            var connection = (Connection)source;
            lock (sessionLock)
            {
                if (connection.stream != null)
                {
                    try
                    {
                        BcpIO.Write(connection.stream, new Bcp.HeartBeat());
                        connection.stream.Flush();
                    }
                    catch
                    {
                    }
                }
            }
        }

        internal void InternalInterrupt()
        {
            Debug.WriteLine("Internal interrupte!");
            foreach (var connection in connections.Values)
            {
                connection.isShutedDown = true;
                if (connection.stream != null)
                {
                    var oldHeartBeatTimer = connection.heartBeatTimer;
                    oldHeartBeatTimer.Change(Timeout.Infinite, Timeout.Infinite);
                    oldHeartBeatTimer.Dispose();
                    connection.heartBeatTimer = null;
                    connection.stream.Dispose();
                    connection.stream = null;
                }
                connection.unconfirmedPackets = new Queue<Bcp.IAcknowledgeRequired>();
            }
            connectionState = SessionState.Unavailable;
            packQueue = new Queue<Bcp.IAcknowledgeRequired>();
            connections.Clear();
            Release();
            EventHandler interruptedEventHandler = Interrupted;
            if (interruptedEventHandler != null)
            {
                interruptedEventHandler(this, EventArgs.Empty);
            }
        }

        public void Interrupt()
        {
            lock (sessionLock)
            {
                InternalInterrupt();
            }
        }

        public void ShutDown()
        {
            lock (sessionLock)
            {
                CheckShutDown();
            }
        }

        public void Send(IList<ArraySegment<Byte>> buffer)
        {
            Debug.WriteLine("Send Message: " + BcpUtil.ArraySegmentListToString(buffer));
            lock (sessionLock)
            {
                Enqueue(new Bcp.Data(buffer));
            }
        }

        internal void AddStream(uint connectionId, Stream stream)
        {
            lock (sessionLock)
            {
                uint oldLastConnectionId = lastConnectionId;
                if (connections.Count >= Bcp.MaxConnectionsPerSession ||
                    ActiveConnectionNum() >= Bcp.MaxActiveConnectionsPerSession)
                {
                    if (stream != null)
                    {
                        stream.Dispose();
                    }
                }
                if (connectionId < oldLastConnectionId ||
                    connectionId - oldLastConnectionId + connections.Count >= Bcp.MaxConnectionsPerSession)
                {
                    InternalInterrupt();
                }
                else
                {
                    if (connectionId > oldLastConnectionId + 1)
                    {
                        for (uint id = oldLastConnectionId + 1; id < connectionId; ++id)
                        {
                            if (!connections.ContainsKey(id))
                            {
                                Connection c = NewConnection();
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
                        connection = NewConnection();
                        connections.Add(connectionId, connection);
                    }
                    lastConnectionId = connectionId;
                    if (connection.stream == null && stream != null)
                    {
                        connection.stream = stream;
                        AddOpenConnection(connection);
                        var newHeartBeatTimer = new Timer(HeartBeatEvent, connection, Bcp.HeartBeatDelayMilliseconds, Bcp.HeartBeatDelayMilliseconds);
                        connection.heartBeatTimer = newHeartBeatTimer;
                        StartReceive(connectionId, connection);
                    }
                    else
                    {
                        if (stream != null)
                        {
                            stream.Dispose();
                        }
                    }
                }
            }
            Debug.WriteLine("After add stream sendingQueue count: " + sendingConnectionQueue.Count());
        }

        internal int ActiveConnectionNum()
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

