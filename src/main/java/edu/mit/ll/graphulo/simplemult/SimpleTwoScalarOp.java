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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;


/**
 * A simple abstract class for matrix multiplication or element-wise multiplication
 * that returns zero or one entry per multiply and whose logic depends only on the Values.
 * The multiply methods are marked final for safety.
 * <p>
 *   Can also be used for one-element Apply, if passed a fixed Value operand
 *   on the left or right of the multiplication inside iterator options.
 *   Defaults to the left side of the multiplication; pass {@value #REVERSE} if the right is desired.
 */
public abstract class SimpleTwoScalarOp extends SimpleApply implements MultiplyOp, EWiseOp {

  /** Implements simple multiply logic. Returning null means no entry is emitted. */
  public abstract Value multiply(Value Aval, Value Bval);


  public static final String
      FIXED_VALUE = "simpleFixedValue",
      REVERSE = "simpleReverse";

  /** For use in one-element applies. Subclasses must complete this method,
   * because we don't know the name of the concrete class, because this method is static.
   * Defaults to setting constant on the left if reverse is false. */
  protected static IteratorSetting addOptionsToIteratorSetting(
      IteratorSetting itset, boolean reverse, Value fixedValue) {
//    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
//    itset.addOption(ApplyIterator.APPLYOP, this.getClass().getName());
    itset.addOption(ApplyIterator.APPLYOP + ApplyIterator.OPT_SUFFIX + FIXED_VALUE, new String(fixedValue.get()));
    itset.addOption(ApplyIterator.APPLYOP + ApplyIterator.OPT_SUFFIX + REVERSE, Boolean.toString(reverse));
    return itset;
  }

  private Value fixedValue = null;
  private boolean reverse = false;

  /** Pass unrecognized options to the parent class. */
  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    Map<String,String> extraOpts = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String k = entry.getKey(), v = entry.getValue();
      switch (k) {
        case FIXED_VALUE:
          fixedValue = new Value(v.getBytes());
          break;
        case REVERSE:
          reverse = Boolean.parseBoolean(v);
          break;
        default:
          extraOpts.put(k,v);
          break;
      }
    }
    super.init(extraOpts, env);
  }

  @Override
  public final Value simpleApply(Key k, Value v) {
    return reverse ? multiply(v, fixedValue) : multiply(fixedValue, v);
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
    Value v = reverse ? multiply(Bval, ATval) : multiply(ATval, Bval);
    return v == null ? Collections.<Entry<Key,Value>>emptyIterator() : Iterators.singletonIterator((Entry<Key, Value>) new SimpleImmutableEntry<>(k, v));
  }

  @Override
  public final Iterator<? extends Entry<Key, Value>> multiply(ByteSequence Mrow, ByteSequence McolF, ByteSequence McolQ, Value Aval, Value Bval) {
    Key k = new Key(Mrow.getBackingArray(), McolF.getBackingArray(),
        McolQ.getBackingArray(), GraphuloUtil.EMPTY_BYTES, System.currentTimeMillis());
    Value v = reverse ? multiply(Bval, Aval) : multiply(Aval, Bval);
    return v == null ? Collections.<Entry<Key,Value>>emptyIterator() : Iterators.singletonIterator((Entry<Key, Value>) new SimpleImmutableEntry<>(k, v));
  }
}
