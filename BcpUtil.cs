using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Bcp
{
    class BcpUtil
    {
        public class DescendingComparer<T> : IComparer<T> where T : IComparable<T>
        {
            public int Compare(T x, T y)
            {
                return y.CompareTo(x);
            }
        }

        public static string ArraySegmentListToString(IList<ArraySegment<byte>> buffers)
        {
            var stringBuffer = new StringBuilder();
            foreach (var buffer in buffers)
            {
                stringBuffer.Append(Encoding.Default.GetString(buffer.ToArray()));
            }
            return stringBuffer.ToString();
        }
    }
}
