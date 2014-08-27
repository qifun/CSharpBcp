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
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace Bcp
{
    class Program
    {
        static Object demoLock = new Object();
        volatile static String received = "Start client success!";

        class ClientDemo : BcpClient
        {
            public ClientDemo()
                : base()
            {
                RegisterEvent();
            }

            public ClientDemo(byte[] sessionId)
                : base(sessionId)
            {
                RegisterEvent();
            }

            private void RegisterEvent()
            {
                this.Unavailable += HandleUnavailableEvent;
                this.Available += HandleAvailableEvent;
                this.ShutedDown += HandleShutedDownEvent;
                this.Interrupted += HandleInterruptedEvent;
                this.Received += OnReceived;
            }

            protected override Socket Connect()
            {
                try
                {
                    EndPoint ipEndPoint = new IPEndPoint(IPAddress.Parse("192.168.1.20"), 3333);
                    Socket socket = new Socket(ipEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                    socket.Connect(ipEndPoint);
                    return socket;
                }
                catch
                {
                    throw new SocketException();
                }
            }

            private void HandleUnavailableEvent(object sender, EventArgs e)
            {
                Console.WriteLine();
                Console.WriteLine("Client unavailable, connecting......");
                Console.WriteLine();
            }

            private void HandleAvailableEvent(object sender, EventArgs e)
            {
                Console.WriteLine("Client avalable!");
            }

            private void HandleShutedDownEvent(object sender, EventArgs e)
            {
                Console.WriteLine("Client ShutedDown!");
            }

            private void HandleInterruptedEvent(object sender, EventArgs e)
            {
                Console.WriteLine("Client Interrupted!");
            }

            private void OnReceived(object sender, ReceivedEventArgs e)
            {
                lock (demoLock)
                {
                    IList<ArraySegment<byte>> buffers = e.Buffers;
                    ArraySegment<byte> ping = buffers[0];
                    received = UTF8Encoding.Default.GetString(ping.Array);
                    Monitor.Pulse(demoLock);
                }
            }
        }

        static void Main(string[] args)
        {
            ThreadPool.SetMinThreads(4, 4);
            var clientDemo = new ClientDemo();
            byte[] sessionId = clientDemo.SessionId;
            uint count = 0;

            while (true)
            {
                lock (demoLock)
                {
                    if (count != 3)
                    {
                        Console.WriteLine(received);
                        Monitor.Wait(demoLock);
                    }
                    else
                    {                       
                        Console.WriteLine("Renew client!");
                        clientDemo.Renew();
                        Monitor.Wait(demoLock);
                    }
                }
                ++count;
            }
        }
    }
}
