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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace Bcp
{
    public abstract class BcpServer
    {
        private Dictionary<string, BcpServer.Session> sessions = new Dictionary<string, Session>();
        private Object serverLock = new Object();

        internal sealed class Connection : BcpSession.Connection
        {
        }

        protected sealed class Session : BcpSession
        {
            private byte[] sessionId;
            private BcpServer bcpServer;

            public Session(BcpServer bcpServer, byte[] sessionId)
            {
                this.sessionId = sessionId;
                this.bcpServer = bcpServer;
            }

            internal override BcpSession.Connection NewConnection()
            {
                return new BcpServer.Connection();
            }

            internal override sealed void Release()
            {
                string sessionKey = Convert.ToBase64String(sessionId);
                bcpServer.sessions.Remove(sessionKey);
            }

            internal override sealed void Busy(BcpSession.Connection connection)
            {
            }

            internal override sealed void Idle(BcpSession.Connection connection)
            {
            }

            internal override sealed void Close(BcpSession.Connection connection)
            {
            }

            public EventHandler Accepted;

            internal void RaiseAccepted()
            {
                EventHandler acceptedEventHandler = Accepted;
                if (acceptedEventHandler != null)
                {
                    acceptedEventHandler(this, EventArgs.Empty);
                }
            }
        }

        protected abstract BcpServer.Session NewSession(byte[] sessionId);

        protected void AddIncomingSocket(Stream stream)
        {
            BcpDelegate.ProcessReadHead processReadHead = delegate(Bcp.ConnectionHead connectionHead)
            {
                var sessionId = connectionHead.SessionId;
                string sessionKey = Convert.ToBase64String(sessionId);
                var connectionId = connectionHead.ConnectionId;
                Debug.WriteLine("BcpServer add incomming socket, sessionId: " + sessionId + ", connectionId: " + connectionId);
                lock (serverLock)
                {
                    BcpServer.Session session;
                    if (sessions.TryGetValue(sessionKey, out session))
                    {
                    }
                    else
                    {
                        session = NewSession(sessionId);
                        sessions.Add(sessionKey, session);
                        session.RaiseAccepted();
                    }
                    if (connectionHead.IsRenew)
                    {
                        session.RenewSession();
                    }
                    session.AddStream(connectionId, stream);
                    Debug.WriteLine("Server added stream!");
                }
            };
            BcpDelegate.ExceptionHandler exceptionHandler = delegate(Exception e)
            {
                Debug.WriteLine("BcpServer add incomming stream exception: " + e.Message);
            };
            TimerCallback readTimeoutCallback = delegate(Object source)
            {
                stream.Dispose();
                exceptionHandler(new Exception());
            };
            Bcp.ReadState readState = new Bcp.ReadState();
            readState.readTimeoutTimer = new Timer(readTimeoutCallback, null, Bcp.ReadingTimeoutMilliseconds, Bcp.ReadingTimeoutMilliseconds);
            BcpIO.ReadHead(stream, readState, processReadHead, exceptionHandler);
        }

    }
}
