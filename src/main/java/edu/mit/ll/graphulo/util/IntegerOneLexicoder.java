/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.mit.ll.graphulo.util;

import org.apache.accumulo.core.client.lexicoder.impl.AbstractLexicoder;

import java.nio.charset.StandardCharsets;

/**
 * A lexicoder for unsigned integers. The encoding sorts Integer.MIN_VALUE first and Integer.MAX_VALUE last. The encoding sorts -2 before -1. It corresponds to
 * the sort order of Integer.
 */
public class IntegerOneLexicoder extends AbstractLexicoder<Integer> {

  @Override
  public byte[] encode(Integer i) {
    return encodeUIL(i);
  }

  private static final byte[] STRING_ONE = "1".getBytes(StandardCharsets.UTF_8);

  @Override
  public Integer decode(byte[] b) {
    // This concrete implementation is provided for binary compatibility with 1.6; it can be removed in 2.0. See ACCUMULO-3789.
    return super.decode(b);
  }

  @Override
  protected Integer decodeUnchecked(byte[] data, int offset, int len) {
    if( data.length == 0 || arrayEqualsOne(data, offset, len) )
      return 1;
    return decodeUncheckedUIL(data, offset, len);
  }

  private static boolean arrayEqualsOne(
      final byte[] first,
      final int firstOffset,
      final int firstLength
  ) {
    if( firstLength != STRING_ONE.length ) return false;

    for( int index = 0; index < firstLength; ++index )
      if (first[firstOffset + index] != STRING_ONE[index])
        return false;

    return true;
  }

  private byte[] encodeUIL(Integer i) {
    int shift = 56;
    int index;
    int prefix = i < 0 ? 0xff : 0x00;

    for (index = 0; index < 4; index++) {
      if (((i >>> shift) & 0xff) != prefix)
        break;

      shift -= 8;
    }

    byte ret[] = new byte[5 - index];
    ret[0] = (byte) (4 - index);
    for (index = 1; index < ret.length; index++) {
      ret[index] = (byte) (i >>> shift);
      shift -= 8;
    }

    if (i < 0)
      ret[0] = (byte) (8 - ret[0]);

    return ret;

  }

    private Integer decodeUncheckedUIL(byte[] data, int offset, int len) {

    if (data[offset] < 0 || data[offset] > 8)
      throw new IllegalArgumentException("Unexpected length " + (0xff & data[offset]));

    int i = 0;
    int shift = 0;

    for (int idx = (offset + len) - 1; idx >= offset + 1; idx--) {
      i += (data[idx] & 0xffL) << shift;
      shift += 8;
    }

    // fill in 0xff prefix
    if (data[offset] > 4)
      i |= -1 << ((8 - data[offset]) << 3);

    return i;
  }

}
