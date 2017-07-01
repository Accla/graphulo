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
package edu.mit.ll.graphulo.tricount;

import org.apache.accumulo.core.client.lexicoder.impl.AbstractLexicoder;

import java.util.Arrays;

import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_BYTES;

/**
 * A lexicoder for signed integers. The encoding sorts Integer.MIN_VALUE first and Integer.MAX_VALUE last. The encoding sorts -2 before -1. It corresponds to
 * the sort order of Integer.
 */
public class IntegerEmptyLexicoder extends AbstractLexicoder<Integer> {

  @Override
  public byte[] encode(Integer i) {
    if (i == 1) return EMPTY_BYTES;
    return new byte[] {
        (byte) (i >> 24),
        (byte) (i >> 16),
        (byte) (i >> 8),
                i.byteValue()
    };
  }

  @Override
  public Integer decode(byte[] b) {
    // This concrete implementation is provided for binary compatibility with 1.6; it can be removed in 2.0. See ACCUMULO-3789.
    return super.decode(b);
  }

  @Override
  protected Integer decodeUnchecked(byte[] data, int offset, int len) {
    if( len == 0 )
      return 1;
    if( len != 4 )
      throw new RuntimeException("cannot parse int value (offset "+offset+", length "+len+": "+ Arrays.toString(data));
    return data[offset] << 24 | (data[offset+1] & 0xFF) << 16 | (data[offset+2] & 0xFF) << 8 | (data[offset+3] & 0xFF);
  }
}
