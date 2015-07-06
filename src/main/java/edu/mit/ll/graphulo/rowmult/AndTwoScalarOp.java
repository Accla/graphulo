package edu.mit.ll.graphulo.rowmult;

import edu.mit.ll.graphulo.simplemult.SimpleTwoScalarOp;
import org.apache.accumulo.core.data.Value;

/**
 * Return 1 regardless of input values.
 * Todo: could unify with AndEWiseX.
 */
public class AndTwoScalarOp extends SimpleTwoScalarOp {
  private static final Value ONE = new Value("1".getBytes());
  @Override
  public Value multiply(Value Aval, Value Bval) {
    return ONE;
  }
}