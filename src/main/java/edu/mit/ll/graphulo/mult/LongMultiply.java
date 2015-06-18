package edu.mit.ll.graphulo.mult;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;

/**
 * Decode values as Long objects, multiply and re-encode the result.
 */
public class LongMultiply extends SimpleMultiply {
  private static final TypedValueCombiner.Encoder<Long> encoder = new LongCombiner.StringEncoder();

  @Override
  public Value multiply(Value ATval, Value Bval) {
    return new Value(encoder.encode(
        encoder.decode(ATval.get()) * encoder.decode(Bval.get())
    ));
  }
}
