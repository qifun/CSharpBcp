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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using System.Collections.Generic;
using System.Threading;
using System.Net.Sockets;
using System.Net;
using System.Text;
using System.Diagnostics;
using System.Runtime.Remoting.Messaging;
using Qifun.Bcp;

namespace BcpTest
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
                AddIncomingSocket(newStream);
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
            protected override BcpServer.Session NewSession(byte[] sessionId)
            {
                BcpServer.Session session = new BcpServer.Session(this, sessionId);
                session.Received += delegate(object sender, BcpSession.ReceivedEventArgs e)
                {
                    lock (testLock)
                    {
                        IList<ArraySegment<byte>> buffers = e.Buffers;
                        Debug.WriteLine("Server received ping!");
                        ArraySegment<byte> ping = buffers[0];
                        serverResult = UTF8Encoding.Default.GetString(ping.Array);
                        byte[] pong = new UTF8Encoding(true).GetBytes("pong");
                        IList<ArraySegment<byte>> sendBuffer = new List<ArraySegment<byte>>();
                        ArraySegment<byte> pingArraySegment = new ArraySegment<byte>(pong, 0, pong.Length);
                        sendBuffer.Add(pingArraySegment);
                        session.Send(sendBuffer);
                        Monitor.Pulse(testLock);
                    }
                };
                return session;
            }
        }

        class PingPongClint : BcpClient
        {

            private EndPoint localEndPoint;

            public PingPongClint(EndPoint localEndPoint)
            {
                this.localEndPoint = localEndPoint;
                this.Received += OnReceived;
            }

            protected override Socket Connect()
            {
                try
                {
                    Debug.WriteLine("Connecting...");
                    EndPoint ep = localEndPoint;
                    Socket socket = new Socket(ep.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                    socket.Connect(ep);
                    return socket;
                }
                catch
                {
                    throw new SocketException();
                }
            }

            private void OnReceived(object sender, ReceivedEventArgs e)
            {
                lock (testLock)
                {
                    IList<ArraySegment<byte>> buffers = e.Buffers;
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
            protected override BcpServer.Session NewSession(byte[] sessionId)
            {
                BcpServer.Session session = new BcpServer.Session(this, sessionId);
                session.Received += delegate(object sender, BcpSession.ReceivedEventArgs e)
                {
                    lock (testLock)
                    {
                        IList<ArraySegment<byte>> buffers = e.Buffers;
                        ArraySegment<byte> ping = buffers[0];
                        serverResult = UTF8Encoding.Default.GetString(ping.Array);
                        Monitor.Pulse(testLock);
                    }
                };
                return session;
            }
        }

        class CloseConnectionClinet : BcpClient
        {

            private EndPoint localEndPoint;

            public CloseConnectionClinet(EndPoint localEndPoint)
            {
                this.localEndPoint = localEndPoint;
                this.Available += OnAvailable;
            }

            protected override Socket Connect()
            {
                try
                {
                    Debug.WriteLine("Connecting...");
                    EndPoint ep = localEndPoint;
                    Socket socket = new Socket(ep.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                    socket.Connect(ep);
                    clientSocket = socket;
                    return socket;
                }
                catch
                {
                    throw new SocketException();
                }
            }

            private void OnAvailable(object sender, EventArgs e)
            {
                lock (testLock)
                {
                    Monitor.Pulse(testLock);
                }
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

    [TestClass]
    public class SeqSendTest
    {
        static Object testLock = new Object();
        volatile static List<string> serverReceivedResult = new List<string>();
        static Socket clientSocket = null;

        class CloseConnectionServer : TestServer
        {
            protected override BcpServer.Session NewSession(byte[] sessionId)
            {
                BcpServer.Session session = new BcpServer.Session(this, sessionId);
                session.Received += delegate(object sender, BcpSession.ReceivedEventArgs e)
                {
                    lock (testLock)
                    {
                        IList<ArraySegment<byte>> buffers = e.Buffers;
                        string receivedString = UTF8Encoding.Default.GetString(buffers[0].Array);
                        Debug.WriteLine("Test server received string: " + receivedString);
                        serverReceivedResult.Add(receivedString);
                        Monitor.Pulse(testLock);
                    }
                };
                return session;
            }
        }

        class CloseConnectionClinet : BcpClient
        {

            private EndPoint localEndPoint;

            public CloseConnectionClinet(EndPoint localEndPoint)
            {
                this.localEndPoint = localEndPoint;
                this.Available += HandleAvailableEvent;
            }

            protected override Socket Connect()
            {
                try
                {
                    Debug.WriteLine("Connecting...");
                    EndPoint ep = localEndPoint;
                    Socket socket = new Socket(ep.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                    socket.Connect(ep);
                    clientSocket = socket;
                    return socket;
                }
                catch
                {
                    throw new SocketException();
                }
            }

            private void HandleAvailableEvent(object sender, EventArgs e)
            {
                lock (testLock)
                {
                    Monitor.Pulse(testLock);
                }
            }

            public void SendString(string buffer)
            {
                byte[] bufferBytes = new UTF8Encoding(true).GetBytes(buffer);
                ArraySegment<byte> oneArraySegment = new ArraySegment<byte>(bufferBytes, 0, bufferBytes.Length);
                IList<ArraySegment<byte>> sendBuffer = new List<ArraySegment<byte>>();
                sendBuffer.Add(oneArraySegment);
                Send(sendBuffer);
            }
        }

        [TestMethod]
        public void SeqSend()
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

            client.SendString("a");
            client.SendString("b");

            lock (testLock)
            {
                while (serverReceivedResult.Count < 2)
                {
                    Monitor.Wait(testLock);
                }
            }

            clientSocket.Close();
            client.SendString("c");
            client.SendString("d");

            lock (testLock)
            {
                while (serverReceivedResult.Count < 4)
                {
                    Monitor.Wait(testLock);
                }
            }

            List<string> result = new List<string>();
            result.Add("a");
            result.Add("b");
            result.Add("c");
            result.Add("d");
            CollectionAssert.AreEquivalent(serverReceivedResult, result);

            client.ShutDown();
            server.Clear();
        }
    }

    public class ClientInterrupeTest
    {
        static Object testLock = new Object();
        volatile static bool clientInterrupteResult = false;

        class InterrupteServer : TestServer
        {
            protected override BcpServer.Session NewSession(byte[] sessionId)
            {
                return new BcpServer.Session(this, sessionId);
            }
        }

        class InterrupteClient : BcpClient
        {

            private EndPoint localEndPoint;

            public InterrupteClient(EndPoint localEndPoint)
            {
                this.localEndPoint = localEndPoint;
                this.Interrupted += HandleInterruptedEvent;
            }

            protected override Socket Connect()
            {
                try
                {
                    Debug.WriteLine("Connecting...");
                    EndPoint ep = localEndPoint;
                    Socket socket = new Socket(ep.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                    socket.Connect(ep);
                    socket.Shutdown(SocketShutdown.Receive);
                    return socket;
                }
                catch
                {
                    throw new SocketException();
                }
            }

            private void HandleInterruptedEvent(object sender, EventArgs e)
            {
                lock (testLock)
                {
                    clientInterrupteResult = true;
                    Monitor.Pulse(testLock);
                }
            }
        }

        [TestMethod]
        public void ClientInterrupte()
        {
            var server = new InterrupteServer();
            var client = new InterrupteClient(server.LocalEndPoint);

            lock (testLock)
            {
                while (clientInterrupteResult == false)
                {
                    Monitor.Wait(testLock);
                }
            }
            Assert.IsTrue(clientInterrupteResult);
            client.ShutDown();
            server.Clear();
        }
    }

    [TestClass]
    public class BcpCryptoTest
    {
        static Object testLock = new Object();
        volatile static String serverResult = null;
        volatile static String clientResult = null;

        class BcpXorServer : TestServer
        {
            protected override BcpServer.Session NewSession(byte[] sessionId)
            {
                BcpServer.Session session = new BcpServer.Session(this, sessionId);
                session.SetCrypto(new BcpXor(), 178);
                session.Received += delegate(object sender, BcpSession.ReceivedEventArgs e)
                {
                    lock (testLock)
                    {
                        IList<ArraySegment<byte>> buffers = e.Buffers;
                        Debug.WriteLine("Server received ping!");
                        ArraySegment<byte> ping = buffers[0];
                        serverResult = Encoding.UTF8.GetString(ping.Array);
                        byte[] pong = new UTF8Encoding(true).GetBytes("是Xor!");
                        IList<ArraySegment<byte>> sendBuffer = new List<ArraySegment<byte>>();
                        ArraySegment<byte> pingArraySegment = new ArraySegment<byte>(pong, 0, pong.Length);
                        sendBuffer.Add(pingArraySegment);
                        session.Send(sendBuffer);
                        Monitor.Pulse(testLock);
                    }
                };
                return session;
            }
        }

        class BcpXorClint : BcpClient
        {
            private EndPoint localEndPoint;

            public BcpXorClint(EndPoint localEndPoint)
            {
                this.localEndPoint = localEndPoint;
                this.Received += OnReceived;
            }

            protected override Socket Connect()
            {
                try
                {
                    Debug.WriteLine("Connecting...");
                    EndPoint ep = localEndPoint;
                    Socket socket = new Socket(ep.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                    socket.Connect(ep);
                    return socket;
                }
                catch
                {
                    throw new SocketException();
                }
            }

            private void OnReceived(object sender, ReceivedEventArgs e)
            {
                lock (testLock)
                {
                    IList<ArraySegment<byte>> buffers = e.Buffers;
                    ArraySegment<byte> pong = buffers[0];
                    clientResult = Encoding.UTF8.GetString(pong.Array);
                    Monitor.Pulse(testLock);
                }
            }
        }

        [TestMethod]
        public void BcpXor()
        {
            var server = new BcpXorServer();
            var client = new BcpXorClint(server.LocalEndPoint);
            client.SetCrypto(new BcpXor(), 178);
            byte[] ping = new UTF8Encoding(true).GetBytes("你的密码是什么?");
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
            Assert.AreEqual(serverResult, "你的密码是什么?");
            Assert.AreEqual(clientResult, "是Xor!");
            client.ShutDown();
            server.Clear();
        }
    }
}
