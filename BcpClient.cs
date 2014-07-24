using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.IO;
using System.Runtime.Remoting.Messaging;
using System.Diagnostics;
using System.Net.Sockets;

namespace Bcp
{
    public abstract class BcpClient : BcpSession
    {
        private Timer reconnectTimer;
        private Timer idleTimer;
        private byte[] sessionId;
        private uint nextConnectionId = 0;
        private bool isConnecting = false;
        private bool isShutedDown = false;
        private bool isRenew = false;

        public BcpClient()
        {
            lock (SessionLock)
            {
                Random random = new Random();
                sessionId = new byte[Bcp.NumBytesSessionId];
                random.NextBytes(sessionId);
                IncreaseConnection();
            }
        }

        public BcpClient(byte[] sessionId)
        {
            lock (SessionLock)
            {
                isRenew = true;
                this.sessionId = (byte[])sessionId.Clone();
                renewSessionConnect();
            }
        }

        public byte[] SessionId { get { return (byte[])sessionId.Clone(); } }

        new internal class Connection : BcpSession.Connection
        {
            public Timer busyTimer;
            public Bcp.ConnectionState connectionState = Bcp.ConnectionState.ConnectionIdle;
        }

        internal override sealed BcpSession.Connection NewConnection()
        {
            return new BcpClient.Connection();
        }

        protected abstract Socket Connect();

        private delegate Stream AsycConnectCaller();

        internal override sealed void Release()
        {
            isShutedDown = true;
            if (reconnectTimer != null)
            {
                reconnectTimer.Change(Timeout.Infinite, Timeout.Infinite);
                reconnectTimer.Dispose();
                reconnectTimer = null;
            }
            if (idleTimer != null)
            {
                idleTimer.Change(Timeout.Infinite, Timeout.Infinite);
                idleTimer.Dispose();
                idleTimer = null;
            }
        }

        internal override sealed void Busy(BcpSession.Connection busyConnection)
        {
            var newBusyConnection = (BcpClient.Connection)busyConnection;
            if (reconnectTimer != null)
            {
                reconnectTimer.Change(Timeout.Infinite, Timeout.Infinite);
                reconnectTimer.Dispose();
                reconnectTimer = null;
            }
            var oldBusyTimer = newBusyConnection.busyTimer;
            if (oldBusyTimer == null)
            {
                var newBusyTimer = new Timer(BusyEvent, busyConnection, 0, Bcp.BusyTimeoutMilliseconds);
                newBusyConnection.busyTimer = newBusyTimer;
                newBusyConnection.connectionState = Bcp.ConnectionState.ConnectionBusy;
                bool isExistIdleConnection = false;
                foreach (var connection in Connections.Values)
                {
                    var newConnection = (BcpClient.Connection)connection;
                    if (newConnection.stream != null && newConnection.connectionState == Bcp.ConnectionState.ConnectionIdle)
                    {
                        isExistIdleConnection = true;
                        break;
                    }
                }
                if (!(Connections.Count() > 1 && isExistIdleConnection))
                {
                    if (idleTimer != null)
                    {
                        idleTimer.Change(Timeout.Infinite, Timeout.Infinite);
                        idleTimer.Dispose();
                        idleTimer = null;
                    }
                }
            }
        }

        private void BusyEvent(object source)
        {
            lock (SessionLock)
            {
                var busyConnection = (BcpClient.Connection)source;
                if (busyConnection.stream != null)
                {
                    if (busyConnection.busyTimer != null)
                    {
                        busyConnection.busyTimer.Change(Timeout.Infinite, Timeout.Infinite);
                        busyConnection.busyTimer.Dispose();
                        busyConnection.busyTimer = null;
                    }
                    busyConnection.connectionState = Bcp.ConnectionState.ConnectionBusy;
                }
                IncreaseConnection();
            }
        }

        internal override sealed void Idle(BcpSession.Connection idleConnection)
        {
            var newIdleConnection = (BcpClient.Connection)idleConnection;
            if (newIdleConnection.busyTimer != null)
            {
                newIdleConnection.busyTimer.Change(Timeout.Infinite, Timeout.Infinite);
                newIdleConnection.busyTimer.Dispose();
                newIdleConnection.busyTimer = null;
            }
            newIdleConnection.connectionState = Bcp.ConnectionState.ConnectionIdle;
            CheckFinishConnection();
        }

        internal override sealed void Close(BcpSession.Connection closeConnection)
        {
            var newCloseConnection = (BcpClient.Connection)closeConnection;
            var connectionSize = Connections.Count();
            if (newCloseConnection.busyTimer != null)
            {
                newCloseConnection.busyTimer.Change(Timeout.Infinite, Timeout.Infinite);
                newCloseConnection.busyTimer.Dispose();
                newCloseConnection.busyTimer = null;
            }
            var isConnectionAllClosed = true;
            foreach (var connection in Connections.Values)
            {
                if (!(connection == closeConnection || connection.stream == null))
                {
                    isConnectionAllClosed = false;
                    break;
                }
            }
            if (isConnectionAllClosed && connectionSize < Bcp.MaxConnectionsPerSession)
            {
                StartReconnectTimer();
            }
            if (connectionSize >= Bcp.MaxConnectionsPerSession && isConnectionAllClosed)
            {
                InternalInterrupt();
            }
        }

        private Stream InternalConnect()
        {
            Socket socket = null;
            Stream stream = null;
            try
            {
                socket = Connect();
                socket.Blocking = true;
                socket.NoDelay = true;
                socket.ReceiveTimeout = (int)Bcp.ReadingTimeoutMilliseconds;
                socket.SendTimeout = (int)Bcp.WritingTimeoutMilliseconds;
                stream = new NetworkStream(socket);
            }
            catch (Exception e)
            {
                Debug.WriteLine("Connect error: " + e);
                lock (SessionLock)
                {
                    isConnecting = false;
                    if (!isShutedDown)
                    {
                        StartReconnectTimer();
                    }
                }
            }
            return stream;
        }

        private void IncreaseConnection()
        {
            Debug.WriteLine("Client increase connection.");
            var activeConnectionNum = 0;
            foreach (var connection in Connections.Values)
            {
                if (connection.stream != null)
                {
                    activeConnectionNum += 1;
                }
            }
            bool isAllConnectionSlow = true;
            foreach (BcpClient.Connection connection in Connections.Values)
            {
                if (!(connection.stream == null || connection.connectionState == Bcp.ConnectionState.ConnectionSlow))
                {
                    isAllConnectionSlow = false;
                    break;
                }
            }
            if (!isConnecting &&
                Connections.Count() < Bcp.MaxConnectionsPerSession &&
                activeConnectionNum < Bcp.MaxActiveConnectionsPerSession &&
                isAllConnectionSlow)
            {
                isConnecting = true;
                var connectionId = nextConnectionId + 1;
                nextConnectionId += 1;
                AsycConnectCaller asyncConnectCaller = new AsycConnectCaller(InternalConnect);
                asyncConnectCaller.BeginInvoke(new AsyncCallback(AfterConnect), connectionId);
            }
        }

        private void renewSessionConnect()
        {
            isConnecting = true;
            var connectionId = nextConnectionId + 1;
            nextConnectionId += 1;
            AsycConnectCaller asyncConnectCaller = new AsycConnectCaller(InternalConnect);
            asyncConnectCaller.BeginInvoke(new AsyncCallback(AfterConnect), connectionId);
        }

        private void AfterConnect(IAsyncResult ar)
        {
            Debug.WriteLine("Handle after connect!");
            AsyncResult result = (AsyncResult)ar;
            AsycConnectCaller caller = (AsycConnectCaller)result.AsyncDelegate;
            uint connectionId = (uint)ar.AsyncState;
            Stream stream = caller.EndInvoke(ar);
            Debug.WriteLine("Connect Success!");
            lock (SessionLock)
            {
                if (!isShutedDown)
                {
                    BcpIO.WriteHead(stream, new Bcp.ConnectionHead(sessionId, isRenew, connectionId));
                    AddStream(connectionId, stream);
                    Debug.WriteLine("Client added stream!");
                    isConnecting = false;
                }
                else
                {
                    if (stream != null)
                    {
                        stream.Dispose();
                    }
                }
                if (stream != null)
                {
                    stream.Flush();
                }
            }
        }

        private void CheckFinishConnection()
        {
            if (Connections.Count() > 1)
            {
                foreach (BcpClient.Connection connection in Connections.Values)
                {
                    if (connection.stream != null && connection.connectionState == Bcp.ConnectionState.ConnectionIdle)
                    {
                        if (idleTimer == null)
                        {
                            TimerCallback idleTimerCallback = delegate(object source)
                            {
                                lock (SessionLock)
                                {
                                    foreach (KeyValuePair<uint, BcpSession.Connection> connectionKeyValue in Connections)
                                    {
                                        var toFinishConnectionId = connectionKeyValue.Key;
                                        var toFinishConnection = (BcpClient.Connection)connectionKeyValue.Value;
                                        if (connection.stream != null &&
                                            connection.connectionState == Bcp.ConnectionState.ConnectionIdle)
                                        {
                                            FinishConnection(toFinishConnectionId, toFinishConnection);
                                            break;
                                        }
                                    }
                                    idleTimer = null;
                                }
                            };
                            var newIdleTimer = new Timer(idleTimerCallback, null, 0, Bcp.IdleTimeoutMilliseconds);
                            connection.heartBeatTimer = newIdleTimer;
                        }
                        break;
                    }
                }
            }
        }

        private void StartReconnectTimer()
        {
            if (reconnectTimer == null)
            {
                TimerCallback busyTimerCallback = delegate(Object source)
                {
                    lock (SessionLock)
                    {
                        IncreaseConnection();
                        reconnectTimer = null;
                    }
                };
                var newBusyTimer = new Timer(busyTimerCallback, null, 0, Bcp.BusyTimeoutMilliseconds);
                reconnectTimer = newBusyTimer;
            }
        }

    }
}
