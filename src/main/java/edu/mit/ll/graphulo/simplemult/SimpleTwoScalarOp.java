package edu.mit.ll.graphulo.simplemult;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.apply.SimpleApply;
import edu.mit.ll.graphulo.ewise.EWiseOp;
import edu.mit.ll.graphulo.rowmult.MultiplyOp;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import static edu.mit.ll.graphulo.apply.EWiseToApplyAdapter.FixSide;

/**
 * A simple abstract class for matrix multiplication or element-wise multiplication
 * that returns zero or one entry per multiply and whose logic depends only on the Values.
 * The multiply methods are marked final for safety.
 * <p>
 *   Can also be used for one-element Apply, if passed a fixed Value operand
 *   on the left or right of the multiplication inside iterator options.
 */
public abstract class SimpleTwoScalarOp extends SimpleApply implements MultiplyOp, EWiseOp {

  /** Implements simple multiply logic. Returning null means no entry is emitted. */
  public abstract Value multiply(Value ATval, Value Bval);


  public static final String
      FIX_SIDE = "fixSide",
      FIXED_VALUE = "fixedValue";

  /** Subclasses must complete this method,
   * because we don't know the name of the concrete class, because this method is static. */
  protected static IteratorSetting addOptionsToIteratorSetting(
      IteratorSetting itset, FixSide fixSide, Value fixedValue) {
//    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
//    itset.addOption(ApplyIterator.APPLYOP, this.getClass().getName());
    itset.addOption(ApplyIterator.APPLYOP+ApplyIterator.OPT_SUFFIX+FIX_SIDE, fixSide.name());
    itset.addOption(ApplyIterator.APPLYOP + ApplyIterator.OPT_SUFFIX + FIXED_VALUE, new String(fixedValue.get()));
    return itset;
  }

  private FixSide fixSide = FixSide.FIX_RIGHT;
  private Value fixedValue = null;

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    super.init(options, env);
    if (options.containsKey(FIXED_VALUE))
      fixedValue = new Value(options.get(FIXED_VALUE).getBytes());
    if (options.containsKey(FIX_SIDE))
      fixSide = FixSide.valueOf(options.get(FIX_SIDE));
  }

  @Override
  public final Value simpleApply(Key k, Value v) {
    switch (fixSide) {
      case FIX_LEFT:
        return multiply(fixedValue, v);
      case FIX_RIGHT:
        return multiply(v, fixedValue);
      default:
        throw new AssertionError();
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    super.seek(range, columnFamilies, inclusive);
  }

  @Override
  public final Iterator<? extends Entry<Key, Value>> apply(Key k, Value v1) {
    return super.apply(k, v1);
  }

  @Override
  public final Iterator<? extends Entry<Key, Value>> multiply(ByteSequence Mrow, ByteSequence ATcolF, ByteSequence ATcolQ, ByteSequence BcolF, ByteSequence BcolQ, Value ATval, Value Bval) {
//    System.err.println("Mrow:"+Mrow+" ATcolQ:"+ATcolQ+" BcolQ:"+BcolQ+" ATval:"+ATval+" Bval:"+Bval);
    Key k = new Key(ATcolQ.getBackingArray(), ATcolF.getBackingArray(),
        BcolQ.getBackingArray(), GraphuloUtil.EMPTY_BYTES, System.currentTimeMillis());
    Value v = multiply(ATval, Bval);
    return v == null ? Collections.<Entry<Key,Value>>emptyIterator() : Iterators.singletonIterator((Entry<Key, Value>) new SimpleImmutableEntry<>(k, v));
  }

  @Override
  public final Iterator<? extends Entry<Key, Value>> multiply(ByteSequence Mrow, ByteSequence McolF, ByteSequence McolQ, Value Aval, Value Bval) {
    Key k = new Key(Mrow.getBackingArray(), McolF.getBackingArray(),
        McolQ.getBackingArray(), GraphuloUtil.EMPTY_BYTES, System.currentTimeMillis());
    Value v = multiply(Aval, Bval);
    return v == null ? Collections.<Entry<Key,Value>>emptyIterator() : Iterators.singletonIterator((Entry<Key, Value>) new SimpleImmutableEntry<>(k, v));
  }
}
