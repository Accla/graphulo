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
 * A simple one. Encode ints directly.
 */
public class FixedIntegerLexicoder extends AbstractLexicoder<Integer> {

  public static final FixedIntegerLexicoder INSTANCE = new FixedIntegerLexicoder();

  @Override
  public byte[] encode(Integer i) {
    return new byte[] {
        (byte) (i >> 24),
        (byte) (i >> 16),
        (byte) (i >> 8),
                i.byteValue()
    };
  }

  public void encode(Integer i, byte[] b) {
    b[0] = (byte) (i >> 24);
    b[1] = (byte) (i >> 16);
    b[2] = (byte) (i >> 8);
    b[3] = i.byteValue();
  }

  @Override
  public Integer decode(byte[] b) {
    return super.decode(b);
  }

  @Override
  public Integer decodeUnchecked(byte[] data, int offset, int len) {
//    if( len != 4 )
//      throw new RuntimeException("cannot parse int value (offset "+offset+", length "+len+": "+ Arrays.toString(data));
    return data[offset] << 24 | (data[offset+1] & 0xFF) << 16 | (data[offset+2] & 0xFF) << 8 | (data[offset+3] & 0xFF);
  }
}
