package edu.mit.ll.graphulo.mult;

import edu.mit.ll.graphulo.IMultiplyOp;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;

/**
 * Decode values as Long objects, multiply and re-encode the result.
 */
public class LongMultiply implements IMultiplyOp {

  private static TypedValueCombiner.Encoder<Long> encoder = new LongCombiner.StringEncoder();

  @Override
  public Value multiply(Value v1, Value v2) {
    long l1 = encoder.decode(v1.get());
    long l2 = encoder.decode(v2.get());
    return new Value(encoder.encode(l1*l2));
  }
}
