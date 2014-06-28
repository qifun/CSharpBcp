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
    public static class BcpIO
    {

        private static async Task<uint> readUnisignedVarint(Stream stream, CancellationToken cancellationToken)
        {
            var buffer = new byte[1];
            var i = 0;
            var result = 0U;
            for (; ; )
            {
                var numBytesRead = await stream.ReadAsync(buffer, 0, 1, cancellationToken);
                if (numBytesRead != 1)
                {
                    throw new EndOfStreamException();
                }
                uint b = buffer[0];
                if (i < 32)
                {
                    if (b >= 0x80)
                    {
                        result |= ((b & 0x7f) << i);
                        i += 7;
                    }
                    else
                    {
                        return result | (b << i);
                    }
                }
                else
                {
                    throw new BcpException.VarintTooBig();
                }
            }
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

        public static void Write(Stream stream, Bcp.Acknowledge packet)
        {
            stream.WriteByte(Bcp.Acknowledge.HeadByte);
        }

        public static void Write(Stream stream, Bcp.Renew packet)
        {
            stream.WriteByte(Bcp.Renew.HeadByte);
        }

        public static void Write(Stream stream, Bcp.Finish packet)
        {
            stream.WriteByte(Bcp.Finish.HeadByte);
        }

        public static void Write(Stream stream, Bcp.RetransmissionFinish packet)
        {
            stream.WriteByte(Bcp.RetransmissionFinish.HeadByte);
            writeUnsignedVarint(stream, packet.ConnectionId);
            writeUnsignedVarint(stream, packet.PackId);
        }

        public static void Write(Stream stream, Bcp.RetransmissionData packet)
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

        public static void Write(Stream stream, Bcp.Data packet)
        {
            stream.WriteByte(Bcp.Data.HeadByte);
            writeUnsignedVarint(stream, (uint)packet.Buffers.Sum(buffer => buffer.Count));
            foreach (var buffer in packet.Buffers)
            {
                stream.Write(buffer.Array, buffer.Offset, buffer.Count);
            }
        }

        public static void Write(Stream stream, Bcp.ShutDown packet)
        {
            stream.WriteByte(Bcp.ShutDown.HeadByte);
        }

        public static void Write(Stream stream, Bcp.HeartBeat packet)
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
            Action<Stream, Bcp.IPacket> writeCallback;
            var isSuccess = writeCallbacks.TryGetValue(packet.GetType(), out writeCallback);
            Debug.Assert(isSuccess);
            writeCallback(stream, packet);
        }

        private static async Task readAll(Stream stream, byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            while (offset < count)
            {
                var numBytesRead = await stream.ReadAsync(buffer, offset, count, cancellationToken);
                if (numBytesRead == 0)
                {
                    throw new EndOfStreamException();
                }
                else
                {
                    offset += numBytesRead;
                }
            }
        }

        public static async Task<Bcp.IPacket> Read(Stream stream, CancellationToken cancellationToken)
        {
            var headBuffer = new byte[1];
            var numBytesRead = await stream.ReadAsync(headBuffer, 0, 1, cancellationToken);
            if (numBytesRead != 1)
            {
                throw new EndOfStreamException();
            }
            switch (headBuffer[0])
            {
                case Bcp.Data.HeadByte:
                    {
                        var length = await readUnisignedVarint(stream, cancellationToken);
                        if (length > Bcp.MaxDataSize)
                        {
                            throw new BcpException.DataTooBig();
                        }
                        var buffer = new byte[length];
                        await readAll(stream, buffer, 0, (int)length, cancellationToken);
                        return new Bcp.Data(new[] { (new ArraySegment<byte>(buffer)) });
                    }
                case Bcp.RetransmissionData.HeadByte:
                    {
                        var connectionId = await readUnisignedVarint(stream, cancellationToken);
                        var packId = await readUnisignedVarint(stream, cancellationToken);
                        var length = await readUnisignedVarint(stream, cancellationToken);
                        if (length > Bcp.MaxDataSize)
                        {
                            throw new BcpException.DataTooBig();
                        }
                        var buffer = new byte[length];
                        await readAll(stream, buffer, 0, (int)length, cancellationToken);
                        return new Bcp.RetransmissionData(connectionId, packId, new[] { (new ArraySegment<byte>(buffer)) });
                    }
                case Bcp.RetransmissionFinish.HeadByte:
                    {
                        var connectionId = await readUnisignedVarint(stream, cancellationToken);
                        var packId = await readUnisignedVarint(stream, cancellationToken);
                        return new Bcp.RetransmissionFinish(connectionId, packId);
                    }
                case Bcp.Acknowledge.HeadByte:
                    return new Bcp.Acknowledge();
                case Bcp.Renew.HeadByte:
                    return new Bcp.Renew();
                case Bcp.Finish.HeadByte:
                    return new Bcp.Finish();
                case Bcp.ShutDown.HeadByte:
                    return new Bcp.ShutDown();
                case Bcp.HeartBeat.HeadByte:
                    return new Bcp.HeartBeat();
                default:
                    throw new BcpException.UnknownHeadByte();
            }
        }

        public static async Task<Bcp.ConnectionHead> ReadHead(Stream stream, CancellationToken cancellationToken)
        {
            var sessionId = new byte[Bcp.NumBytesSessionId];
            await readAll(stream, sessionId, 0, Bcp.NumBytesSessionId, cancellationToken);
            var connectionId = await readUnisignedVarint(stream, cancellationToken);
            return new Bcp.ConnectionHead(sessionId, connectionId);
        }


        public static void WriteHead(Stream stream, Bcp.ConnectionHead head)
        {
            stream.Write(head.SessionId, 0, Bcp.NumBytesSessionId);
            writeUnsignedVarint(stream, head.ConnectionId);
        }
    }
}