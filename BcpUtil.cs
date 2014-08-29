/*
 * csharp-bcp
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
                byte[] bytes = new byte[buffer.Count];
                Array.Copy(buffer.Array, buffer.Offset, bytes, 0, buffer.Count);
                stringBuffer.Append(Encoding.Default.GetString(bytes));
            }
            return stringBuffer.ToString();
        }
    }
}
