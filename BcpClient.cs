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

        public BcpClient()
        {
            lock (sessionLock)
            {
                Random random = new Random();
                sessionId = new byte[Bcp.NumBytesSessionId];
                random.NextBytes(sessionId);
                increaseConnection();
            }
        }

        new internal class Connection : BcpSession.Connection
        {
            public Timer busyTimer;
            public Bcp.ConnectionState connectionState = Bcp.ConnectionState.ConnectionIdle;
        }

        internal override sealed BcpSession.Connection newConnection()
        {
            return new BcpClient.Connection();
        }

        protected abstract Socket connect();

        private delegate Stream AsycConnectCaller();

        internal override sealed void release()
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

        internal override sealed void busy(BcpSession.Connection busyConnection)
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
                var newBusyTimer = new Timer(busyEvent, busyConnection, 0, Bcp.BusyTimeoutMilliseconds);
                newBusyConnection.busyTimer = newBusyTimer;
                newBusyConnection.connectionState = Bcp.ConnectionState.ConnectionBusy;
                bool isExistIdleConnection = false;
                foreach (var connection in connections.Values)
                {
                    var newConnection = (BcpClient.Connection)connection;
                    if (newConnection.stream != null && newConnection.connectionState == Bcp.ConnectionState.ConnectionIdle)
                    {
                        isExistIdleConnection = true;
                        break;
                    }
                }
                if (!(connections.Count() > 1 && isExistIdleConnection))
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

        private void busyEvent(object source)
        {
            lock (sessionLock)
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
                increaseConnection();
            }
        }

        internal override sealed void idle(BcpSession.Connection idleConnection)
        {
            var newIdleConnection = (BcpClient.Connection)idleConnection;
            if (newIdleConnection.busyTimer != null)
            {
                newIdleConnection.busyTimer.Change(Timeout.Infinite, Timeout.Infinite);
                newIdleConnection.busyTimer.Dispose();
                newIdleConnection.busyTimer = null;
            }
            newIdleConnection.connectionState = Bcp.ConnectionState.ConnectionIdle;
            checkFinishConnection();
        }

        internal override sealed void close(BcpSession.Connection closeConnection)
        {
            var newCloseConnection = (BcpClient.Connection)closeConnection;
            var connectionSize = connections.Count();
            if (newCloseConnection.busyTimer != null)
            {
                newCloseConnection.busyTimer.Change(Timeout.Infinite, Timeout.Infinite);
                newCloseConnection.busyTimer.Dispose();
                newCloseConnection.busyTimer = null;
            }
            var isConnectionAllClosed = true;
            foreach (var connection in connections.Values)
            {
                if (!(connection == closeConnection || connection.stream == null))
                {
                    isConnectionAllClosed = false;
                    break;
                }
            }
            if (isConnectionAllClosed && connectionSize < Bcp.MaxConnectionsPerSession)
            {
                startReconnectTimer();
            }
            if (connectionSize >= Bcp.MaxConnectionsPerSession && isConnectionAllClosed)
            {
                internalInterrupt();
            }
        }

        private Stream internalConnect()
        {
            Socket socket = null;
            Stream stream = null;
            try
            {
                socket = connect();
                socket.Blocking = true;
                socket.NoDelay = true;
                socket.ReceiveTimeout = (int)Bcp.ReadingTimeoutMilliseconds;
                socket.SendTimeout = (int)Bcp.WritingTimeoutMilliseconds;
                stream = new NetworkStream(socket);
            }
            catch (Exception e)
            {
                Debug.WriteLine("Connect error: " + e);
                lock (sessionLock)
                {
                    startReconnectTimer();
                }
            }
            return stream;
        }

        private void increaseConnection()
        {
            Debug.WriteLine("Client increase connection.");
            var activeConnectionNum = 0;
            foreach (var connection in connections.Values)
            {
                if (connection.stream != null)
                {
                    activeConnectionNum += 1;
                }
            }
            bool isAllConnectionSlow = true;
            foreach (BcpClient.Connection connection in connections.Values)
            {
                if (!(connection.stream == null || connection.connectionState == Bcp.ConnectionState.ConnectionSlow))
                {
                    isAllConnectionSlow = false;
                    break;
                }
            }
            if (!isConnecting &&
                connections.Count() < Bcp.MaxConnectionsPerSession &&
                activeConnectionNum < Bcp.MaxActiveConnectionsPerSession &&
                isAllConnectionSlow)
            {
                isConnecting = true;
                var connectionId = nextConnectionId + 1;
                nextConnectionId += 1;
                AsycConnectCaller asyncConnectCaller = new AsycConnectCaller(internalConnect);
                asyncConnectCaller.BeginInvoke(new AsyncCallback(afterConnect), connectionId);
            }
        }

        private void afterConnect(IAsyncResult ar)
        {
            Debug.WriteLine("Handle after connect!");
            AsyncResult result = (AsyncResult)ar;
            AsycConnectCaller caller = (AsycConnectCaller)result.AsyncDelegate;
            uint connectionId = (uint)ar.AsyncState;
            Stream stream = caller.EndInvoke(ar);
            if (stream != null)
            {
                Debug.WriteLine("Connect Success!");
                lock (sessionLock)
                {
                    if (stream != null)
                    {
                        if (!isShutedDown)
                        {
                            BcpIO.WriteHead(stream, new Bcp.ConnectionHead(sessionId, connectionId));
                            addStream(connectionId, stream);
                            Debug.WriteLine("Client added stream!");
                            isConnecting = false;
                        }
                        else
                        {
                            stream.Dispose();
                        }
                        stream.Flush();
                    }
                }
            }
        }

        private void checkFinishConnection()
        {
            if (connections.Count() > 1)
            {
                foreach (BcpClient.Connection connection in connections.Values)
                {
                    if (connection.stream != null && connection.connectionState == Bcp.ConnectionState.ConnectionIdle)
                    {
                        if (idleTimer == null)
                        {
                            TimerCallback idleTimerCallback = delegate(object source)
                            {
                                lock (sessionLock)
                                {
                                    foreach (KeyValuePair<uint, BcpSession.Connection> connectionKeyValue in connections)
                                    {
                                        var toFinishConnectionId = connectionKeyValue.Key;
                                        var toFinishConnection = (BcpClient.Connection)connectionKeyValue.Value;
                                        if (connection.stream != null &&
                                            connection.connectionState == Bcp.ConnectionState.ConnectionIdle)
                                        {
                                            finishConnection(toFinishConnectionId, toFinishConnection);
                                            break;
                                        }
                                    }
                                    idleTimer = null;
                                }
                            };
                            var newIdleTimer = new Timer(idleTimerCallback, null, 0, Bcp.IdleTimeoutMilliseconds);
                            connection.HeartBeatTimer = newIdleTimer;
                        }
                        break;
                    }
                }
            }
        }

        private void startReconnectTimer()
        {
            if (reconnectTimer == null)
            {
                TimerCallback busyTimerCallback = delegate(Object source)
                {
                    lock (sessionLock)
                    {
                        increaseConnection();
                        reconnectTimer = null;
                    }
                };
                var newBusyTimer = new Timer(busyTimerCallback, null, 0, Bcp.BusyTimeoutMilliseconds);
                reconnectTimer = newBusyTimer;
            }
        }

    }
}
