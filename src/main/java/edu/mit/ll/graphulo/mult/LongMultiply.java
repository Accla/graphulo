package edu.mit.ll.graphulo.mult;

import edu.mit.ll.graphulo.IMultiplyOp;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;

import java.util.Map;

/**
 * Decode values as Long objects, multiply and re-encode the result.
 */
public class LongMultiply extends SimpleMultiply {
  private static TypedValueCombiner.Encoder<Long> encoder = new LongCombiner.StringEncoder();

  @Override
  public Value multiply(Value ATval, Value Bval) {
    return new Value(encoder.encode(
        encoder.decode(ATval.get()) * encoder.decode(Bval.get())
    ));
  }
}
