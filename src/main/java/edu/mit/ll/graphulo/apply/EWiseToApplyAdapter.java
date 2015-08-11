package edu.mit.ll.graphulo.apply;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.ewise.EWiseOp;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * An ApplyOp formed from an EWiseOp by fixing the left or right Value operand of the EWiseOp.
 * Pass the fixed value via iterator options.
 * <p>
 *  All options that are not {@value #EWISEOP} or {@value #FIX_SIDE} or {@value #FIXED_VALUE}
 *  are passed to the MultiplyOp.
 */
public class EWiseToApplyAdapter implements ApplyOp {

  public enum FixSide { FIX_LEFT, FIX_RIGHT }

  public static final String EWISEOP ="eWiseOpForApply",
    FIX_SIDE = "fixSide",
    FIXED_VALUE = "fixedValue";

  private EWiseOp eWiseOp;
  private FixSide fixSide = FixSide.FIX_RIGHT;
  private Value fixedValue;

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    String clazz = options.get(EWISEOP);
    Preconditions.checkArgument(clazz != null, "Required option %s. Given: %s", EWISEOP, options);
    eWiseOp = GraphuloUtil.subclassNewInstance(clazz, EWiseOp.class);

    String valueFixed = options.get(FIXED_VALUE);
    Preconditions.checkArgument(valueFixed != null, "Required option %s. Given: %s", FIXED_VALUE, options);
    fixedValue = new Value(valueFixed.getBytes());

    if (options.containsKey(FIX_SIDE))
      fixSide = FixSide.valueOf(options.get(FIX_SIDE));

    Map<String,String> otherOpts = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String k = entry.getKey();
      if (!k.equals(EWISEOP) && !k.equals(FIX_SIDE) && !k.equals(FIXED_VALUE))
        otherOpts.put(entry.getKey(), entry.getValue());
    }
    eWiseOp.init(otherOpts, env);
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v) {
    ByteSequence
        Mrow = k.getRowData(),
        McolF = k.getColumnFamilyData(),
        McolQ = k.getColumnQualifierData();
    switch (fixSide) {
      case FIX_LEFT:
        return eWiseOp.multiply(Mrow, McolF, McolQ, null, fixedValue, v);
      case FIX_RIGHT:
        return eWiseOp.multiply(Mrow, McolF, McolQ, null, v, fixedValue);
      default:
        throw new AssertionError();
    }
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
  }

}
