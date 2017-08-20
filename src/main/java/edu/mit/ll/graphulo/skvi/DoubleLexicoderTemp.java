package edu.mit.ll.graphulo.skvi;

import org.apache.accumulo.core.iterators.TypedValueCombiner;

import static edu.mit.ll.graphulo.skvi.LongLexicoderTemp.decodeLongUnchecked;
import static edu.mit.ll.graphulo.skvi.LongLexicoderTemp.encodeLong;

/**
 * A lexicoder for preserving the native Java sort order of Double values.
 *
 * Temporary stand-in for Accumulo server 1.6.0 compatibility
 */
public class DoubleLexicoderTemp implements TypedValueCombiner.Encoder<Double> {

  @Override
  public byte[] encode(Double d) {
    long l = Double.doubleToRawLongBits(d);
    if (l < 0)
      l = ~l;
    else
      l = l ^ 0x8000000000000000l;

    return encodeLong(l);
  }

  @Override
  public Double decode(byte[] b) {
    // This concrete implementation is provided for binary compatibility with 1.6; it can be removed in 2.0. See ACCUMULO-3789.
    return decodeUnchecked(b, 0, b.length);
  }

  protected Double decodeUnchecked(byte[] data, int offset, int len) {
    long l = decodeLongUnchecked(data, offset, len);
    if (l < 0)
      l = l ^ 0x8000000000000000l;
    else
      l = ~l;
    return Double.longBitsToDouble(l);
  }

}
