package edu.mit.ll.graphulo.rowmult;

import edu.mit.ll.graphulo.simplemult.SimpleTwoScalarOp;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;

/**
 * Decode values as Long objects, take the max, and re-encode the result.
 */
public class MinTwoScalarOp extends SimpleTwoScalarOp {
  private static final TypedValueCombiner.Encoder<Long> encoder = new LongCombiner.StringEncoder();

  @Override
  public Value multiply(Value ATval, Value Bval) {
    return new Value(encoder.encode(
        Math.min( encoder.decode(ATval.get()), encoder.decode(Bval.get()) )
    ));
  }
}
