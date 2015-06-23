package edu.mit.ll.graphulo.apply;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Force all Values to "1".
 */
public class Abs0Apply extends SimpleApply {
  private static final Value ONE = new Value("1".getBytes());
  @Override
  public Value simpleApply(Key k, Value v) {
    return ONE;
  }
}
