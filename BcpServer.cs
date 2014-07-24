using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
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

            internal override BcpSession.Connection newConnection()
            {
                return new BcpServer.Connection();
            }

            internal override sealed void release()
            {
                string sessionKey = Convert.ToBase64String(sessionId);
                bcpServer.sessions.Remove(sessionKey);
            }

            internal override sealed void busy(BcpSession.Connection connection)
            {
            }

            internal override sealed void idle(BcpSession.Connection connection)
            {
            }

            internal override sealed void close(BcpSession.Connection connection)
            {
            }

            protected abstract void accepted();

            internal void internalAccepted()
            {
                accepted();
            }
        }

        protected abstract BcpServer.Session newSession(byte[] sessionId);

        protected void addIncomingSocket(Stream stream)
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
                        session = newSession(sessionId);
                        sessions.Add(sessionKey, session);
                        session.internalAccepted();
                    }
                    if (connectionHead.IsRenew)
                    {
                        session.renewSession();
                    }
                    session.addStream(connectionId, stream);
                    Debug.WriteLine("Server added stream!");
                }
            };
            BcpDelegate.ExceptionHandler exceptionHandler = delegate(Exception e)
            {
                Debug.WriteLine("BcpServer add incomming stream exception: " + e.Message);
            };
            BcpIO.ReadHead(stream, processReadHead, exceptionHandler);
        }

    }
}
