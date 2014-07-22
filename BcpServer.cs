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
        private static Dictionary<byte[], BcpServer.Session> sessions = new Dictionary<byte[], Session>();
        private Object serverLock = new Object();

        internal class Connection : BcpSession.Connection
        {
        }

        public abstract class Session : BcpSession
        {
            private byte[] sessionId = new byte[Bcp.NumBytesSessionId];

            public Session()
            {
            }

            public Session(byte sessionId)
            {
            }

            override internal BcpSession.Connection newConnection()
            {
                return new BcpServer.Connection();
            }

            internal override void release()
            {
                sessions.Remove(this.sessionId);
            }

            internal override void busy(BcpSession.Connection connection)
            {
            }

            internal override void idle(BcpSession.Connection connection)
            {
            }

            internal override void close(BcpSession.Connection connection)
            {
            }

            public abstract void accepted();

            internal void internalAccepted()
            {
                accepted();
            }
        }

        public abstract BcpServer.Session newSession(byte[] sessionId);

        public void addIncomingSocket(Stream stream)
        {
            BcpDelegate.ProcessReadHead processReadHead = delegate(Bcp.ConnectionHead connectionHead)
            {
                var sessionId = connectionHead.SessionId;
                var connectionId = connectionHead.ConnectionId;
                Debug.WriteLine("BcpServer add incomming socket, sessionId: " + sessionId + ", connectionId: " + connectionId);
                lock (serverLock)
                {
                    BcpServer.Session session;
                    if (sessions.TryGetValue(sessionId, out session))
                    {
                    }
                    else
                    {
                        session = newSession(sessionId);
                        sessions.Add(sessionId, session);
                        session.internalAccepted();
                    }
                    session.addStream(connectionId, stream);
                }
            };
            BcpDelegate.ExceptionHandler exceptionHandler = delegate(Exception e)
            {
                Debug.WriteLine(e.Message);
            };
            BcpIO.ReadHead(stream, processReadHead, exceptionHandler);
        }

    }
}
