package edu.mit.ll.graphulo.ewise;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;

/**
 * Decode Values as Long objects, sum and re-encode the result.
 * Used for EWiseSum.
 * Can be implemented as MultiplyToEWiseAdapter passing in LongSumMultiply
 */
public class LongEWiseSum extends SimpleEWiseX {
  private static final TypedValueCombiner.Encoder<Long> encoder = new LongCombiner.StringEncoder();

  @Override
  public Value multiply(Value Aval, Value Bval) {
    return new Value(encoder.encode(
        encoder.decode(Aval.get()) + encoder.decode(Bval.get())
    ));
  }
}
