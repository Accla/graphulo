package edu.mit.ll.graphulo.ewise;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;

/**
 * Decode Values as Long objects, multiply and re-encode the result.
 * Used for EWiseX instead of TableMult because it uses the row of A.
 * Can be implemented as MultiplyToEWiseAdapter passing in LongTwoScalarOp
 */
public class LongEWiseX extends SimpleEWiseX {
  private static final TypedValueCombiner.Encoder<Long> encoder = new LongCombiner.StringEncoder();

  @Override
  public Value multiply(Value Aval, Value Bval) {
    return new Value(encoder.encode(
        encoder.decode(Aval.get()) * encoder.decode(Bval.get())
    ));
  }
}
