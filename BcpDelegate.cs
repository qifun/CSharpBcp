using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bcp
{
    class BcpDelegate
    {
        public delegate void ProcessRead(Bcp.IPacket packet);

        public delegate void ProcessReadHead(Bcp.ConnectionHead connectionHead);

        public delegate void ExceptionHandler(Exception e);

    }
}
