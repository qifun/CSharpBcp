using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Qifun.Bcp
{
    public interface BcpCrypto
    {
        IList<ArraySegment<Byte>> dataDecrypt(IList<ArraySegment<Byte>> buffer, int key);

        IList<ArraySegment<Byte>> dataEncrypt(IList<ArraySegment<Byte>> buffer, int key);


    }
}
