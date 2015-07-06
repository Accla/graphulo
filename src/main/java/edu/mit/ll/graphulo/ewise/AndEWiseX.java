package edu.mit.ll.graphulo.ewise;

import org.apache.accumulo.core.data.Value;

/**
 * Return 1 regardless of input values.
 * Can be implemented as MultiplyToEWiseAdapter passing in AndTwoScalarOp.
 */
public class AndEWiseX extends SimpleEWiseX {
  private static final Value ONE = new Value("1".getBytes());
  @Override
  public Value multiply(Value Aval, Value Bval) {
    return ONE;
  }
}
