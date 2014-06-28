using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Bcp
{
    public class BcpException : Exception
    {
        
        public class UnknownHeadByte : BcpException
        {
            public UnknownHeadByte()
                : base()
            {
            }
        }

        public class DataTooBig : BcpException
        {
            public DataTooBig()
                : base()
            {
            }
        }

        public class VarintTooBig : BcpException
        {
            public VarintTooBig()
                : base()
            {
            }
        }
    }
}
