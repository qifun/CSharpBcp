using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Qifun.Bcp
{
    public class BcpXor : BcpCrypto
    {
        public IList<ArraySegment<Byte>> dataDecrypt(IList<ArraySegment<Byte>> buffer, int key)
        {
            IList<ArraySegment<Byte>> decryptBuffer = new List<ArraySegment<Byte>>();
            for (int i = 0; i < buffer.Count; i++)
            {
                ArraySegment<Byte> bufferLine = buffer[i];
                for (int j = bufferLine.Offset; j < (bufferLine.Offset + bufferLine.Count); j++)
                {
                    bufferLine.Array[j] = (Byte)(bufferLine.Array[j] ^ key);
                }
                decryptBuffer.Add(bufferLine);
            }
            return decryptBuffer;
        }

        public IList<ArraySegment<Byte>> dataEncrypt(IList<ArraySegment<Byte>> buffer, int key)
        {
            IList<ArraySegment<Byte>> encryptBuffer = new List<ArraySegment<Byte>>();
            for (int i = 0; i < buffer.Count; i++)
            {
                ArraySegment<Byte> bufferLine = buffer[i];
                for (int j = bufferLine.Offset; j < (bufferLine.Offset + bufferLine.Count); j++)
                {
                    bufferLine.Array[j] = (Byte)(bufferLine.Array[j] ^ key);
                }
                encryptBuffer.Add(bufferLine);
            }
            return encryptBuffer;
        }
    }
}
