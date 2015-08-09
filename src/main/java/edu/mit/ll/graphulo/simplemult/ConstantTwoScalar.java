package edu.mit.ll.graphulo.simplemult;

import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Return a constant regardless of input values.  Default "1".
 */
public class ConstantTwoScalar extends SimpleTwoScalar {
  public static final String CONSTANT = "constant";

  /** For use as an ApplyOp. */
  public static IteratorSetting iteratorSetting(int priority, Value constant) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, ConstantTwoScalar.class.getName());
    for (Map.Entry<String, String> entry : optionMap(constant, "").entrySet())
      itset.addOption(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX + entry.getKey(), entry.getValue());
    return itset;
  }

  /** For use as a MultiplyOp or EWiseOp. */
  public static Map<String,String> optionMap(Value constant, String newVisibility) {
    Map<String,String> map = new HashMap<>();
    map.put(CONSTANT, new String(constant.get()));
    if (newVisibility != null && !newVisibility.isEmpty())
      map.put(NEW_VISIBILITY, newVisibility);
    return map;
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

  @Override
  public ConstantTwoScalar deepCopy(IteratorEnvironment env) {
    ConstantTwoScalar copy = (ConstantTwoScalar) super.deepCopy(env);
    copy.constant = constant;
    return copy;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("ConstantTwoScalar");
    io.setDescription("A Combiner that replaces all entries that match on row through column visibility with a constant Value");
    io.addNamedOption(CONSTANT, "The constant Value");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    if (options.containsKey(CONSTANT))
      new Value(options.get(CONSTANT).getBytes());
    return super.validateOptions(options);
  }
}