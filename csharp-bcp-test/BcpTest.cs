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

        Socket serverSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

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

        public void Clear()
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
            protected override BcpServer.Session newSession(byte[] sessionId)
            {
                return new Session(sessionId);
            }

            protected class Session : BcpServer.Session
            {
                public Session()
                {
                }

                public Session(byte[] sesssionId)
                {
                }

                protected override void accepted()
                {
                }

                protected override void unavailable()
                {
                }

                protected override void available()
                {
                }

                protected override void shutedDown()
                {
                }

                protected override void interrupted()
                {
                }

                protected override void received(IList<ArraySegment<byte>> buffers)
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
                        Send(sendBuffer);
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

            protected override Stream connect()
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

            protected override void unavailable()
            {
            }

            protected override void available()
            {
            }

            protected override void shutedDown()
            {
            }

            protected override void interrupted()
            {
            }

            protected override void received(IList<ArraySegment<byte>> buffers)
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
            client.Send(sendBuffer);
            lock (testLock)
            {
                while (serverResult == null || clientResult == null)
                {
                    Monitor.Wait(testLock);
                }
            }
            Assert.AreEqual(serverResult, "ping");
            Assert.AreEqual(clientResult, "pong");
            client.ShutDown();
            server.Clear();
        }
    }

    [TestClass]
    public class CloseConnectionTest
    {
        static Object testLock = new Object();
        volatile static String serverResult = null;
        static Socket clientSocket = null;

        class CloseConnectionServer : TestServer
        {
            protected override BcpServer.Session newSession(byte[] sessionId)
            {
                return new Session(sessionId);
            }

            protected class Session : BcpServer.Session
            {
                public Session()
                {
                }

                public Session(byte[] sesssionId)
                {
                }

                protected override void accepted()
                {
                }

                protected override void unavailable()
                {
                }

                protected override void available()
                {
                }

                protected override void shutedDown()
                {
                }

                protected override void interrupted()
                {
                }

                protected override void received(IList<ArraySegment<byte>> buffers)
                {
                    lock (testLock)
                    {
                        Debug.WriteLine("Server received ping!");
                        ArraySegment<byte> ping = buffers[0];
                        serverResult = UTF8Encoding.Default.GetString(ping.Array);
                        Monitor.Pulse(testLock);
                    }
                }
            }
        }

        class CloseConnectionClinet : BcpClient
        {

            private EndPoint localEndPoint;

            public CloseConnectionClinet(EndPoint localEndPoint)
            {
                this.localEndPoint = localEndPoint;
            }

            protected override Stream connect()
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
                    clientSocket = socket;
                    return stream;
                }
                catch
                {
                    throw new SocketException();
                }
            }

            protected override void unavailable()
            {
            }

            protected override void available()
            {
                lock (testLock)
                {
                    Monitor.Pulse(testLock);
                }
            }

            protected override void shutedDown()
            {
            }

            protected override void interrupted()
            {
            }

            protected override void received(IList<ArraySegment<byte>> buffers)
            {
            }
        }

        [TestMethod]
        public void CloseConnection()
        {
            var server = new CloseConnectionServer();
            var client = new CloseConnectionClinet(server.LocalEndPoint);

            lock (testLock)
            {
                while (clientSocket == null)
                {
                    Monitor.Wait(testLock);
                }
            }
            clientSocket.Close();
            byte[] sendMessage = new UTF8Encoding(true).GetBytes("Hello bcp-server!");
            IList<ArraySegment<byte>> sendBuffer = new List<ArraySegment<byte>>();
            ArraySegment<byte> pingArraySegment = new ArraySegment<byte>(sendMessage, 0, sendMessage.Length);
            sendBuffer.Add(pingArraySegment);
            client.Send(sendBuffer);
            lock (testLock)
            {
                while (serverResult == null)
                {
                    Monitor.Wait(testLock);
                }
            }
            Assert.AreEqual(serverResult, "Hello bcp-server!");

            client.ShutDown();
            server.Clear();
        }
    }
}
