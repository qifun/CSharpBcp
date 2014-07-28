using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Bcp
{
    public abstract class BcpServer
    {
        private Dictionary<string, BcpServer.Session> sessions = new Dictionary<string, Session>();
        private Object serverLock = new Object();

        internal class Connection : BcpSession.Connection
        {
        }

        protected abstract class Session : BcpSession
        {
            public Session()
            {
            }

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

            protected abstract void Accepted();

            internal void InternalAccepted()
            {
                Accepted();
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
                        session.InternalAccepted();
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
