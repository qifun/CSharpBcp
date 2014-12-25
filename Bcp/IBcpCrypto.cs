using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Qifun.Bcp
{
    public interface IBcpCrypto
    {
        IList<ArraySegment<Byte>> DataDecrypt(IList<ArraySegment<Byte>> buffer, int key);

        IList<ArraySegment<Byte>> DataEncrypt(IList<ArraySegment<Byte>> buffer, int key);

    }
}
