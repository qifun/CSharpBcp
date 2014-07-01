using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
namespace Bcp
{
    abstract class BcpSession
    {

        static private bool between(uint low, uint high, uint test)
        {
            if (low < high)
            {
                return test >= low && test < high;
            }
            else if (low > high)
            {
                return test >= low || test < high;
            }
            else
            {
                return false;
            }
        }

        private sealed class IDSetIsFullException : Exception { }

        private sealed class IDSet : HashSet<uint>
        {

            private const uint MaxUnconfirmedIds = 1024;

            private uint lowID;

            private uint highID;

            private void compat()
            {
                while (base.Contains(lowID))
                {
                    base.Remove(lowID);
                    lowID += 1;
                }
            }

            public new void Add(uint id)
            {
                if (between(lowID, highID, id))
                {
                    base.Add(id);
                    compat();
                }
                else if (between(lowID, lowID + MaxUnconfirmedIds, id))
                {
                    highID += 1;
                    base.Add(id);
                    compat();
                }
            }

            public bool IsReceived(uint id)
            {
                if (between(lowID, highID, id))
                {
                    return base.Contains(id);
                }
                else if (between(highID, highID + MaxUnconfirmedIds, id))
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }

            bool AllReceivedBelow(uint id)
            {
                return (!(this.Any<uint>())) && lowID == id && highID == id;
            }

        }

        private sealed class Connection
        {
            public Stream stream;
            public uint FinishID;
            public bool IsFinishIDReceived = false;
            public uint NumDataReceived = 0;
            public IDSet ReceiveIDSet;
            public uint NumDataSent = 0;
            public uint NumAcknowledgeReceivedForData = 0;
            public Queue<Bcp.IAcknowledgeRequired> UnconfirmedPackets = new Queue<Bcp.IAcknowledgeRequired>();
        }

        public Queue<Bcp.IAcknowledgeRequired> ResendingPackets = new Queue<Bcp.IAcknowledgeRequired>();

    }
}

