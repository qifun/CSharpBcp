﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Bcp
{
    public static class BcpIO
    {
        private delegate void ProcessReadVarint(uint result);

        private delegate void ProcessReadAll();
        
        static object writeLock = new object();

        private static void readUnsignedVarint(Stream stream, ProcessReadVarint processReadVarint, BcpDelegate.ExceptionHandler exceptionHandler)
        {
            var buffer = new byte[1];
            var i = 0;
            uint result = 0U;
            AsyncCallback asyncCallback = null;
            asyncCallback = asyncResult =>
            {
                int numBytesRead = stream.EndRead(asyncResult);
                if (numBytesRead != 1)
                {
                    exceptionHandler(new EndOfStreamException());
                }
                uint b = buffer[0];
                if (i < 32)
                {
                    if (b >= 0x80)
                    {
                        result |= ((b & 0x7f) << i);
                        i += 7;
                        stream.BeginRead(buffer, 0, 1, asyncCallback, null);
                    }
                    else
                    {
                        result |= (b << i);
                        processReadVarint(result);
                    }
                }
                else
                {
                    exceptionHandler(new BcpException.VarintTooBig());
                }
            };
            stream.BeginRead(buffer, 0, 1, asyncCallback, null);
        }

        private static void writeUnsignedVarint(Stream stream, uint value)
        {
            while ((value & 0xFFFFFF80) != 0)
            {
                stream.WriteByte((byte)((value & 0x7F) | 0x80));
                value >>= 7;
            }
            stream.WriteByte((byte)value);
        }

        private static void Write(Stream stream, Bcp.Acknowledge packet)
        {
            stream.WriteByte(Bcp.Acknowledge.HeadByte);
        }

        private static void Write(Stream stream, Bcp.Renew packet)
        {
            stream.WriteByte(Bcp.Renew.HeadByte);
        }

        private static void Write(Stream stream, Bcp.Finish packet)
        {
            stream.WriteByte(Bcp.Finish.HeadByte);
        }

        private static void Write(Stream stream, Bcp.RetransmissionFinish packet)
        {
            stream.WriteByte(Bcp.RetransmissionFinish.HeadByte);
            writeUnsignedVarint(stream, packet.ConnectionId);
            writeUnsignedVarint(stream, packet.PackId);
        }

        private static void Write(Stream stream, Bcp.RetransmissionData packet)
        {
            stream.WriteByte(Bcp.RetransmissionData.HeadByte);
            writeUnsignedVarint(stream, packet.ConnectionId);
            writeUnsignedVarint(stream, packet.PackId);
            writeUnsignedVarint(stream, (uint)packet.Buffers.Sum(buffer => buffer.Count));
            foreach (var buffer in packet.Buffers)
            {
                stream.Write(buffer.Array, buffer.Offset, buffer.Count);
            }
        }

        private static void Write(Stream stream, Bcp.Data packet)
        {
            stream.WriteByte(Bcp.Data.HeadByte);
            writeUnsignedVarint(stream, (uint)packet.Buffers.Sum(buffer => buffer.Count));
            foreach (var buffer in packet.Buffers)
            {
                stream.Write(buffer.Array, buffer.Offset, buffer.Count);
            }
        }

        private static void Write(Stream stream, Bcp.ShutDown packet)
        {
            stream.WriteByte(Bcp.ShutDown.HeadByte);
        }

        private static void Write(Stream stream, Bcp.HeartBeat packet)
        {
            stream.WriteByte(Bcp.HeartBeat.HeadByte);
        }

        private static Dictionary<Type, Action<Stream, Bcp.IPacket>> initializeWriteCallbacks()
        {
            var dictionary = new Dictionary<Type, Action<Stream, Bcp.IPacket>>();
            dictionary.Add(typeof(Bcp.Acknowledge), (stream, packet) => Write(stream, (Bcp.Acknowledge)packet));
            dictionary.Add(typeof(Bcp.Data), (stream, packet) => Write(stream, (Bcp.Data)packet));
            dictionary.Add(typeof(Bcp.Finish), (stream, packet) => Write(stream, (Bcp.Finish)packet));
            dictionary.Add(typeof(Bcp.RetransmissionData), (stream, packet) => Write(stream, (Bcp.RetransmissionData)packet));
            dictionary.Add(typeof(Bcp.RetransmissionFinish), (stream, packet) => Write(stream, (Bcp.RetransmissionFinish)packet));
            dictionary.Add(typeof(Bcp.ShutDown), (stream, packet) => Write(stream, (Bcp.ShutDown)packet));
            dictionary.Add(typeof(Bcp.HeartBeat), (stream, packet) => Write(stream, (Bcp.HeartBeat)packet));
            dictionary.Add(typeof(Bcp.Renew), (stream, packet) => Write(stream, (Bcp.Renew)packet));
            return dictionary;
        }

        private static Dictionary<Type, Action<Stream, Bcp.IPacket>> writeCallbacks = initializeWriteCallbacks();

        public static void Write(Stream stream, Bcp.IPacket packet)
        {
            lock (writeLock)
            {
                Action<Stream, Bcp.IPacket> writeCallback;
                var isSuccess = writeCallbacks.TryGetValue(packet.GetType(), out writeCallback);
                Debug.Assert(isSuccess);
                writeCallback(stream, packet);
            }
        }

        private static void readAll(
            Stream stream, 
            byte[] buffer, 
            int offset, 
            int count, 
            ProcessReadAll processReadAll, 
            BcpDelegate.ExceptionHandler exceptionHandler)
        {
            AsyncCallback asyncCallback = null;
            asyncCallback = asyncResult =>
            {
                int numBytesRead = stream.EndRead(asyncResult);
                if (numBytesRead == 0)
                {
                    exceptionHandler(new EndOfStreamException());
                }
                else
                {
                    offset += numBytesRead;
                }
                if (offset < count)
                {
                    stream.BeginRead(buffer, offset, count, asyncCallback, null);
                }
                else
                {
                    processReadAll();
                }
            };
            stream.BeginRead(buffer, offset, count, asyncCallback, null);
        }

        public static void Read(Stream stream, BcpDelegate.ProcessRead processRead, BcpDelegate.ExceptionHandler exceptionHandler)
        {
            var headBuffer = new byte[1];
            AsyncCallback asyncCallback = null;
            asyncCallback = asyncResult =>
            {
                int numBytesRead = stream.EndRead(asyncResult);
                if (numBytesRead != 1)
                {
                    throw new EndOfStreamException();
                }
                switch (headBuffer[0])
                {
                    case Bcp.Data.HeadByte:
                        {
                            ProcessReadVarint processReadLength = delegate(uint length)
                            {
                                if (length > Bcp.MaxDataSize)
                                {
                                    throw new BcpException.DataTooBig();
                                }
                                var buffer = new byte[length];
                                ProcessReadAll processReadAll = delegate()
                                {
                                    processRead(new Bcp.Data(new[] { (new ArraySegment<byte>(buffer)) }));
                                };
                                readAll(stream, buffer, 0, (int)length, processReadAll, exceptionHandler);
                            };
                            readUnsignedVarint(stream, processReadLength, exceptionHandler);
                            break;
                        }
                    case Bcp.RetransmissionData.HeadByte:
                        {
                            ProcessReadVarint processReadConnectionId = delegate(uint connectionId)
                            {
                                ProcessReadVarint processReadPackId = delegate(uint packId)
                                {
                                    ProcessReadVarint processReadLength = delegate(uint length)
                                    {
                                        if (length > Bcp.MaxDataSize)
                                        {
                                            throw new BcpException.DataTooBig();
                                        }
                                        var buffer = new byte[length];
                                        ProcessReadAll processReadAll = delegate()
                                        {
                                            processRead(new Bcp.RetransmissionData(connectionId, packId, new[] { (new ArraySegment<byte>(buffer)) }));
                                        };
                                        readAll(stream, buffer, 0, (int)length, processReadAll, exceptionHandler);

                                    };
                                    readUnsignedVarint(stream, processReadLength, exceptionHandler);
                                };
                                readUnsignedVarint(stream, processReadPackId, exceptionHandler);
                            };
                            readUnsignedVarint(stream, processReadConnectionId, exceptionHandler);
                            break;
                        }
                    case Bcp.RetransmissionFinish.HeadByte:
                        {
                            ProcessReadVarint processReadConnectionId = delegate(uint connectionId)
                            {
                                ProcessReadVarint processReadPackId = delegate(uint packId)
                                {
                                    processRead(new Bcp.RetransmissionFinish(connectionId, packId));
                                };
                                readUnsignedVarint(stream, processReadPackId, exceptionHandler);
                            };
                            readUnsignedVarint(stream, processReadConnectionId, exceptionHandler);
                            break;
                        }
                    case Bcp.Acknowledge.HeadByte:
                        processRead(new Bcp.Acknowledge());
                        break;
                    case Bcp.Renew.HeadByte:
                        processRead(new Bcp.Renew());
                        break;
                    case Bcp.Finish.HeadByte:
                        processRead(new Bcp.Finish());
                        break;
                    case Bcp.ShutDown.HeadByte:
                        processRead(new Bcp.ShutDown());
                        break;
                    case Bcp.HeartBeat.HeadByte:
                        processRead(new Bcp.HeartBeat());
                        break;
                    default:
                        throw new BcpException.UnknownHeadByte();
                }
            };
            stream.BeginRead(headBuffer, 0, 1, asyncCallback, null);
        }

        public static void ReadHead(Stream stream, BcpDelegate.ProcessReadHead processReadHead, BcpDelegate.ExceptionHandler exceptionHandler)
        {
            var sessionId = new byte[Bcp.NumBytesSessionId];
            ProcessReadAll processReadAll = delegate()
            {
                ProcessReadVarint processReadConnectionId = delegate(uint connectionId)
                {
                    processReadHead(new Bcp.ConnectionHead(sessionId, connectionId));
                };
                readUnsignedVarint(stream, processReadConnectionId, null);
            };
            readAll(stream, sessionId, 0, Bcp.NumBytesSessionId, processReadAll, exceptionHandler);
        }

        public static void WriteHead(Stream stream, Bcp.ConnectionHead head)
        {
            stream.Write(head.SessionId, 0, Bcp.NumBytesSessionId);
            writeUnsignedVarint(stream, head.ConnectionId);
        }
    }
}