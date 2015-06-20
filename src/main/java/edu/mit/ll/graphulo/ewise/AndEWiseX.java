package edu.mit.ll.graphulo.ewise;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;

/**
 * Return 1 regardless of input values.
 */
public class AndEWiseX extends SimpleEWiseX {
  private static TypedValueCombiner.Encoder<Long> encoder = new LongCombiner.StringEncoder();
  private static final Value ONE = new Value("1".getBytes());
  @Override
  public Value multiply(Value Aval, Value Bval) {
    return ONE;
  }
}
