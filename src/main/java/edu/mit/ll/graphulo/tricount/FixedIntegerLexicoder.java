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
