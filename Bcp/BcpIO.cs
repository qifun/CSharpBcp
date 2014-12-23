/*
 * CSharpBcp
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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace Qifun.Bcp
{
    public static class BcpIO
    {
        private delegate void ProcessReadVarint(uint result);

        private delegate void ProcessReadAll();

        private static void ReadUnsignedVarint(
            Stream stream,
            Bcp.ReadState readState,
            ProcessReadVarint processReadVarint,
            BcpDelegate.ExceptionHandler exceptionHandler)
        {
            var buffer = new byte[1];
            var i = 0;
            uint result = 0U;
            AsyncCallback asyncCallback = null;
            asyncCallback = asyncResult =>
            {
                try
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
                            stream.BeginRead(buffer, 0, 1, asyncCallback, readState);
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
                }
                catch (Exception e)
                {
                    exceptionHandler(e);
                }
            };
            try
            {
                stream.BeginRead(buffer, 0, 1, asyncCallback, readState);
            }
            catch (Exception e)
            {
                exceptionHandler(e);
            }
        }

        private static void WriteUnsignedVarint(Stream stream, uint value)
        {
            while ((value & 0xFFFFFF80) != 0)
            {
                stream.WriteByte((byte)((value & 0x7F) | 0x80));
                value >>= 7;
            }
            stream.WriteByte((byte)value);
        }

        public static void Write(Stream stream, Bcp.Acknowledge packet)
        {
            try
            {
                stream.WriteByte(Bcp.Acknowledge.HeadByte);
            }
            catch
            {
                stream.Close();
            }
        }

        public static void Write(Stream stream, Bcp.Finish packet)
        {
            try
            {
                stream.WriteByte(Bcp.Finish.HeadByte);
            }
            catch
            {
                stream.Close();
            }
        }

        public static void Write(Stream stream, Bcp.RetransmissionFinish packet)
        {
            try
            {
                stream.WriteByte(Bcp.RetransmissionFinish.HeadByte);
                WriteUnsignedVarint(stream, packet.ConnectionId);
                WriteUnsignedVarint(stream, packet.PackId);
            }
            catch
            {
                stream.Close();
            }
        }

        public static void Write(Stream stream, Bcp.RetransmissionData packet)
        {
            try
            {
                stream.WriteByte(Bcp.RetransmissionData.HeadByte);
                WriteUnsignedVarint(stream, packet.ConnectionId);
                WriteUnsignedVarint(stream, packet.PackId);
                WriteUnsignedVarint(stream, (uint)packet.Buffers.Sum(buffer => buffer.Count));
                foreach (var buffer in packet.Buffers)
                {
                    stream.Write(buffer.Array, buffer.Offset, buffer.Count);
                }
            }
            catch
            {
                stream.Close();
            }
        }

        public static void Write(Stream stream, Bcp.Data packet)
        {
            try
            {
                stream.WriteByte(Bcp.Data.HeadByte);
                WriteUnsignedVarint(stream, (uint)packet.Buffers.Sum(buffer => buffer.Count));
                foreach (var buffer in packet.Buffers)
                {
                    stream.Write(buffer.Array, buffer.Offset, buffer.Count);
                }
            }
            catch
            {
                stream.Close();
            }
        }

        public static void Write(Stream stream, Bcp.ShutDown packet)
        {
            try
            {
                stream.WriteByte(Bcp.ShutDown.HeadByte);
            }
            catch
            {
                stream.Close();
            }
        }

        public static void Write(Stream stream, Bcp.HeartBeat packet)
        {
            try
            {
                stream.WriteByte(Bcp.HeartBeat.HeadByte);
            }
            catch
            {
                stream.Close();
            }
        }

        private static Dictionary<Type, Action<Stream, Bcp.IPacket>> InitializeWriteCallbacks()
        {
            var dictionary = new Dictionary<Type, Action<Stream, Bcp.IPacket>>();
            dictionary.Add(typeof(Bcp.Acknowledge), (stream, packet) => Write(stream, (Bcp.Acknowledge)packet));
            dictionary.Add(typeof(Bcp.Data), (stream, packet) => Write(stream, (Bcp.Data)packet));
            dictionary.Add(typeof(Bcp.Finish), (stream, packet) => Write(stream, (Bcp.Finish)packet));
            dictionary.Add(typeof(Bcp.RetransmissionData), (stream, packet) => Write(stream, (Bcp.RetransmissionData)packet));
            dictionary.Add(typeof(Bcp.RetransmissionFinish), (stream, packet) => Write(stream, (Bcp.RetransmissionFinish)packet));
            dictionary.Add(typeof(Bcp.ShutDown), (stream, packet) => Write(stream, (Bcp.ShutDown)packet));
            dictionary.Add(typeof(Bcp.HeartBeat), (stream, packet) => Write(stream, (Bcp.HeartBeat)packet));
            return dictionary;
        }

        private static Dictionary<Type, Action<Stream, Bcp.IPacket>> writeCallbacks = InitializeWriteCallbacks();

        public static void Write(Stream stream, Bcp.IPacket packet)
        {
            try
            {
                Action<Stream, Bcp.IPacket> writeCallback;
                var isSuccess = writeCallbacks.TryGetValue(packet.GetType(), out writeCallback);
                Debug.Assert(isSuccess);
                writeCallback(stream, packet);
            }
            catch
            {
                stream.Close();
            }
        }

        private static void ReadAll(
            Stream stream,
            Bcp.ReadState readState,
            byte[] buffer,
            int offset,
            int count,
            ProcessReadAll processReadAll,
            BcpDelegate.ExceptionHandler exceptionHandler)
        {
            AsyncCallback asyncCallback = null;
            asyncCallback = asyncResult =>
            {
                try
                {
                    int numBytesRead = stream.EndRead(asyncResult);
                    if (numBytesRead == 0)
                    {
                        exceptionHandler(new EndOfStreamException());
                    }
                    else
                    {
                        offset += numBytesRead;

                        if (offset < count)
                        {
                            stream.BeginRead(buffer, offset, count, asyncCallback, readState);
                        }
                        else
                        {
                            processReadAll();
                        }
                    }
                }
                catch (Exception e)
                {
                    exceptionHandler(e);
                }
            };
            try
            {
                stream.BeginRead(buffer, offset, count, asyncCallback, readState);
            }
            catch (Exception e)
            {
                exceptionHandler(e);
            }
        }

        public static void Read(Stream stream, Bcp.ReadState readState, BcpDelegate.ProcessRead processRead, BcpDelegate.ExceptionHandler exceptionHandler)
        {
            var headBuffer = new byte[1];
            AsyncCallback asyncCallback = null;
            asyncCallback = asyncResult =>
            {
                try
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
                                    ReadAll(stream, readState, buffer, 0, (int)length, processReadAll, exceptionHandler);
                                };
                                ReadUnsignedVarint(stream, readState, processReadLength, exceptionHandler);
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
                                            ReadAll(stream, readState, buffer, 0, (int)length, processReadAll, exceptionHandler);

                                        };
                                        ReadUnsignedVarint(stream, readState, processReadLength, exceptionHandler);
                                    };
                                    ReadUnsignedVarint(stream, readState, processReadPackId, exceptionHandler);
                                };
                                ReadUnsignedVarint(stream, readState, processReadConnectionId, exceptionHandler);
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
                                    ReadUnsignedVarint(stream, readState, processReadPackId, exceptionHandler);
                                };
                                ReadUnsignedVarint(stream, readState, processReadConnectionId, exceptionHandler);
                                break;
                            }
                        case Bcp.Acknowledge.HeadByte:
                            processRead(new Bcp.Acknowledge());
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
                }
                catch (Exception e)
                {
                    exceptionHandler(e);
                }
            };
            try
            {
                stream.BeginRead(headBuffer, 0, 1, asyncCallback, readState);
            }
            catch (Exception e)
            {
                exceptionHandler(e);
            }
        }

        public static void ReadHead(Stream stream,
            Bcp.ReadState readState,
            BcpDelegate.ProcessReadHead processReadHead,
            BcpDelegate.ExceptionHandler exceptionHandler)
        {
            var sessionId = new byte[Bcp.NumBytesSessionId];
            ProcessReadAll processReadAll = delegate()
            {
                ProcessReadVarint processReadIsRenew = delegate(uint isRenew)
                {
                    ProcessReadVarint processReadConnectionId = delegate(uint connectionId)
                    {
                        readState.Cancel();
                        processReadHead(new Bcp.ConnectionHead(sessionId, Convert.ToBoolean(isRenew), connectionId));
                    };
                    ReadUnsignedVarint(stream, readState, processReadConnectionId, exceptionHandler);
                };
                ReadUnsignedVarint(stream, readState, processReadIsRenew, exceptionHandler);
            };
            ReadAll(stream, readState, sessionId, 0, Bcp.NumBytesSessionId, processReadAll, exceptionHandler);
        }

        public static void WriteHead(Stream stream, Bcp.ConnectionHead head)
        {
            try
            {
                stream.Write(head.SessionId, 0, Bcp.NumBytesSessionId);
                WriteUnsignedVarint(stream, Convert.ToUInt32(head.IsRenew));
                WriteUnsignedVarint(stream, head.ConnectionId);
            }
            catch
            {
            }
        }

    }
}