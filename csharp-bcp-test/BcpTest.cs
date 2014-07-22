using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Net.Sockets;
using System.Net;
using System.Text;
using System.Diagnostics;
using System.Runtime.Remoting.Messaging;

namespace Bcp
{
    abstract class TestServer : BcpServer
    {
        public static IPEndPoint ipEndPoint = new IPEndPoint(IPAddress.Loopback, 0);

        static Socket serverSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

        public EndPoint LocalEndPoint;

        public TestServer()
        {
            serverSocket.Bind(ipEndPoint);
            serverSocket.Listen(100);
            LocalEndPoint = serverSocket.LocalEndPoint;
            Debug.WriteLine("Listening: " + serverSocket.LocalEndPoint);
            startAccept();
        }

        private void startAccept()
        {
            serverSocket.BeginAccept(new AsyncCallback(acceptCallback), null);
        }

        private void acceptCallback(IAsyncResult ar)
        {
            try
            {
                Socket newSocket = serverSocket.EndAccept(ar);
                NetworkStream newStream = new NetworkStream(newSocket);
                addIncomingSocket(newStream);
                startAccept();
            }
            catch
            {
            }
        }

        public void clear()
        {
            serverSocket.Dispose();
        }
    }

    [TestClass]
    public class PingPongTest
    {
        static Object testLock = new Object();
        volatile static String serverResult = null;
        volatile static String clientResult = null;

        class PingPongServer : TestServer
        {
            public override BcpServer.Session newSession(byte[] sessionId)
            {
                return new Session(sessionId);
            }

            class Session : BcpServer.Session
            {
                public Session()
                {
                }

                public Session(byte[] sesssionId)
                {
                }

                public override void accepted()
                {
                }

                public override void unavailable()
                {
                }

                public override void available()
                {
                }

                public override void shutedDown()
                {
                }

                public override void interrupted()
                {
                }

                public override void received(IList<ArraySegment<byte>> buffers)
                {
                    lock (testLock)
                    {
                        Debug.WriteLine("Server received ping!");
                        ArraySegment<byte> ping = buffers[0];
                        serverResult = UTF8Encoding.Default.GetString(ping.Array);
                        byte[] pong = new UTF8Encoding(true).GetBytes("pong");
                        IList<ArraySegment<byte>> sendBuffer = new List<ArraySegment<byte>>();
                        ArraySegment<byte> pingArraySegment = new ArraySegment<byte>(pong, 0, pong.Length);
                        sendBuffer.Add(pingArraySegment);
                        send(sendBuffer);
                        Monitor.Pulse(testLock);
                    }
                }

            }
        }

        class PingPongClint : BcpClient
        {

            private EndPoint localEndPoint;

            public PingPongClint(EndPoint localEndPoint)
            {
                this.localEndPoint = localEndPoint;
            }

            public override Stream connect()
            {
                try
                {
                    Debug.WriteLine("Connecting...");
                    NetworkStream stream;
                    EndPoint ep = localEndPoint;
                    Socket socket = new Socket(ep.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                    socket.Connect(ep);
                    socket.Blocking = true;
                    socket.NoDelay = true;
                    socket.ReceiveTimeout = (int)Bcp.ReadingTimeoutMilliseconds;
                    socket.SendTimeout = (int)Bcp.WritingTimeoutMilliseconds;
                    stream = new NetworkStream(socket);
                    return stream;
                }
                catch
                {
                    throw new SocketException();
                }
            }

            public override void unavailable()
            {
            }

            public override void available()
            {
            }

            public override void shutedDown()
            {
            }

            public override void interrupted()
            {
            }

            public override void received(IList<ArraySegment<byte>> buffers)
            {
                lock(testLock)
                {
                    ArraySegment<byte> pong = buffers[0];
                    clientResult = UTF8Encoding.Default.GetString(pong.Array);
                    Monitor.Pulse(testLock);

                }
            }

        }

        [TestMethod]
        public void PingPong()
        {            
            var server = new PingPongServer();
            var client = new PingPongClint(server.LocalEndPoint);
            byte[] ping = new UTF8Encoding(true).GetBytes("ping");
            IList<ArraySegment<byte>> sendBuffer = new List<ArraySegment<byte>>();
            ArraySegment<byte> pingArraySegment = new ArraySegment<byte>(ping, 0, ping.Length);
            sendBuffer.Add(pingArraySegment);
            client.send(sendBuffer);
            lock (testLock)
            {
                while (serverResult == null || clientResult == null)
                {
                    Monitor.Wait(testLock);
                }
            }
            Assert.AreEqual(serverResult, "ping");
            Assert.AreEqual(clientResult, "pong");
            client.shutDown();
            server.clear();
        }
    }
}
