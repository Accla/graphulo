package edu.mit.ll.graphulo.simplemult;

import edu.mit.ll.graphulo.apply.ApplyIterator;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Return a constant regardless of input values.  Default "1".
 */
public class ConstantTwoScalarOp extends SimpleTwoScalarOp {
  public static final String CONSTANT = "constant";

  /** For use as an ApplyOp. */
  public static IteratorSetting iteratorSetting(int priority, Value constant) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, ConstantTwoScalarOp.class.getName());
    for (Map.Entry<String, String> entry : optionMap(constant).entrySet())
      itset.addOption(ApplyIterator.APPLYOP + ApplyIterator.OPT_SUFFIX + entry.getKey(), entry.getValue());
    return itset;
  }

  /** For use as a MultiplyOp or EWiseOp. */
  public static Map<String,String> optionMap(Value constant) {
    return Collections.singletonMap(CONSTANT, new String(constant.get()));
  }

  private Value constant = new Value("1".getBytes());

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    Map<String,String> extraOpts = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String k = entry.getKey(), v = entry.getValue();
      switch (k) {
        case CONSTANT:
          constant = new Value(v.getBytes());
          break;
        default:
          extraOpts.put(k, v);
          break;
      }
    }
    super.init(extraOpts, env);
  }

  @Override
  public Value multiply(Value Aval, Value Bval) {
    return constant;
  }
}