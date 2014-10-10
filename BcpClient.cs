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

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.IO;
using System.Runtime.Remoting.Messaging;
using System.Diagnostics;
using System.Net.Sockets;
using System.Security.Cryptography;

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

        private static readonly RNGCryptoServiceProvider secureRandom = new RNGCryptoServiceProvider();

        public BcpClient()
        {
            lock (sessionLock)
            {
                sessionId = new byte[Bcp.NumBytesSessionId];
                secureRandom.GetBytes(sessionId);
                IncreaseConnection();
            }
        }

        /// <summary>
        /// 崩溃时重置客户端
        /// </summary>
        /// <param name="sessionId"></param>
        public BcpClient(byte[] sessionId)
        {
            lock (sessionLock)
            {
                isShutedDown = false;
                isRenew = true;
                this.sessionId = (byte[])sessionId.Clone();
                RenewSession();
                renewSessionConnect();
            }
        }
        
        /// <summary>
        /// Unavailable太长时间可重置客户端，所有连接都会被关闭，所有数据都会被清除
        /// </summary>
        public void Renew()
        {
            lock (sessionLock)
            {
                isRenew = true;
                nextConnectionId = 0;
                RenewSession();
                renewSessionConnect();
            }
        }

        public byte[] SessionId { get { return (byte[])sessionId.Clone(); } }

        new internal sealed class Connection : BcpSession.Connection
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
            newBusyConnection.connectionState = Bcp.ConnectionState.ConnectionBusy;
            var oldBusyTimer = newBusyConnection.busyTimer;
            if (oldBusyTimer == null)
            {
                var newBusyTimer = new Timer(BusyEvent, busyConnection, Bcp.BusyTimeoutMilliseconds, Bcp.BusyTimeoutMilliseconds);
                newBusyConnection.busyTimer = newBusyTimer;
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

        private void BusyEvent(object source)
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
                socket.SendTimeout = (int)Bcp.WritingTimeoutMilliseconds;
                stream = new NetworkStream(socket);
            }
            catch (Exception e)
            {
                Debug.WriteLine("Connect error: " + e);
                lock (sessionLock)
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
            if (reconnectTimer != null)
            {
                reconnectTimer.Change(Timeout.Infinite, Timeout.Infinite);
                reconnectTimer = null;
            }
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
                AsycConnectCaller asyncConnectCaller = new AsycConnectCaller(InternalConnect);
                asyncConnectCaller.BeginInvoke(new AsyncCallback(AfterConnect), null);
            }
        }

        private void renewSessionConnect()
        {
            isConnecting = true;
            AsycConnectCaller asyncConnectCaller = new AsycConnectCaller(InternalConnect);
            asyncConnectCaller.BeginInvoke(new AsyncCallback(AfterConnect), null);
        }

        private void AfterConnect(IAsyncResult ar)
        {
            Debug.WriteLine("Handle after connect!");
            AsyncResult result = (AsyncResult)ar;
            AsycConnectCaller caller = (AsycConnectCaller)result.AsyncDelegate;
            Stream stream = caller.EndInvoke(ar);
            if (stream != null)
            {
                Debug.WriteLine("Connect Success!");
                lock (sessionLock)
                {
                    if (!isShutedDown)
                    {
                        var connectionId = nextConnectionId + 1;
                        nextConnectionId += 1;
                        BcpIO.WriteHead(stream, new Bcp.ConnectionHead(sessionId, isRenew, connectionId));
                        AddStream(connectionId, stream);
                        Debug.WriteLine("Client added stream!");
                        Debug.WriteLine("Connection num: " + connections.Count);
                        isConnecting = false;
                        isRenew = false;
                    }
                    else
                    {
                        stream.Dispose();
                    }
                    stream.Flush();
                }
            }
        }

        private void CheckFinishConnection()
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
                                            FinishConnection(toFinishConnectionId, toFinishConnection);
                                            break;
                                        }
                                    }
                                    idleTimer.Change(Timeout.Infinite, Timeout.Infinite);
                                    idleTimer.Dispose();
                                    idleTimer = null;
                                }
                            };
                            var newIdleTimer = new Timer(idleTimerCallback, null, Bcp.IdleTimeoutMilliseconds, Bcp.IdleTimeoutMilliseconds);
                            idleTimer = newIdleTimer;
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
                    IncreaseConnection();
                    reconnectTimer = null;
                };
                var newBusyTimer = new Timer(busyTimerCallback, null, Bcp.ReconnectTimeoutMilliseconds, Bcp.ReconnectTimeoutMilliseconds);
                reconnectTimer = newBusyTimer;
            }
        }

    }
}
