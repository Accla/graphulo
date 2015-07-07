package edu.mit.ll.graphulo.simplemult;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.apply.ApplyOp;
import edu.mit.ll.graphulo.ewise.EWiseOp;
import edu.mit.ll.graphulo.rowmult.MultiplyOp;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;


/**
 * A simple abstract class for an operation acting on two scalars.
 * This includes matrix multiplication, element-wise multiplication,
 * element-wise sum, and Combiner-style sum.
 * <p>
 * The requirement is that this operation logic must return zero or one entry per multiply
 * and depend only on Values, not Keys.  More advanced operations should not use this class.
 * Certain methods are marked as final to prevent misuse/confusion.
 * <p>
 *   Can also be used for one-element Apply, if passed a fixed Value operand
 *   on the left or right of the multiplication inside iterator options.
 *   Defaults to the left side of the multiplication; pass {@value #REVERSE} if the right is desired.
 */
public abstract class SimpleTwoScalarOp extends Combiner implements ApplyOp, MultiplyOp, EWiseOp {
  private static final Logger log = LogManager.getLogger(SimpleTwoScalarOp.class);

  //////////////////////////////////////////////////////////////////////////////////
  /** Implements simple multiply logic. Returning null means no entry is emitted. */
  public abstract Value multiply(Value Aval, Value Bval);
  //////////////////////////////////////////////////////////////////////////////////

  public static final String
      FIXED_VALUE = "simpleFixedValue",
      REVERSE = "simpleReverse";

  /** For use in one-element applies. Subclasses must complete this method,
   * because we don't know the name of the concrete class, because this method is static.
   * Defaults to setting constant on the left if reverse is false. */
  protected static IteratorSetting addOptionsToIteratorSetting(IteratorSetting itset, boolean reverse, Value fixedValue) {
//    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
//    itset.addOption(ApplyIterator.APPLYOP, this.getClass().getName());
    itset.addOption(ApplyIterator.APPLYOP + ApplyIterator.OPT_SUFFIX + FIXED_VALUE, new String(fixedValue.get()));
    itset.addOption(ApplyIterator.APPLYOP + ApplyIterator.OPT_SUFFIX + REVERSE, Boolean.toString(reverse));
    return itset;
  }

  private Value fixedValue = null;
  private boolean reverse = false;

  /** Used in ApplyIterator.
   * This is different than {@link #init(SortedKeyValueIterator, Map, IteratorEnvironment)},
   * when this class is used as a Combiner. */
  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String k = entry.getKey(), v = entry.getValue();
      switch (k) {
        case FIXED_VALUE:
          fixedValue = new Value(v.getBytes());
          break;
        case REVERSE:
          reverse = Boolean.parseBoolean(v);
          break;
        case ALL_OPTION:
        case COLUMNS_OPTION:
          break;
        default:
          log.warn("Unrecognized option: "+k+" -> "+v);
          break;
      }
    }
  }

  /** The Combiner SKVI init. Calls the ApplyOp/MultiplyOp/EWiseOp init from here.
   * Marked final for safety. */
  @Override
  public final void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    Map<String,String> notCombinerOpts = new HashMap<>(), combinerOpts = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String k = entry.getKey(), v = entry.getValue();
      switch (k) {
        case ALL_OPTION:
        case COLUMNS_OPTION:
          combinerOpts.put(k,v);
          break;
        default:
          notCombinerOpts.put(k, v);
          log.warn("Unrecognized option: "+k+" -> "+v);
          break;
      }
    }
    init(notCombinerOpts, env);             // Call the ApplyOp init.
    super.init(source, combinerOpts, env);  // Setup Combiner parent.
  }

  /** Feel free to override this method to obtain additional information from seek calls, for the case of ApplyOps. */
  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
  }

  @Override
  public final Iterator<? extends Entry<Key, Value>> apply(Key k, Value v1) {
    Value v2 = reverse ? multiply(v1, fixedValue) : multiply(fixedValue, v1);
    return v2 == null ? Collections.<Map.Entry<Key,Value>>emptyIterator() : Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, v2));
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
    // todo - Aval xor Bval could be null, if emitNoMatchA or emitNoMatchB are true in TwoTableIterator. Not sure the best way to architect. Deferring.
    Key k = new Key(Mrow.getBackingArray(), McolF.getBackingArray(),
        McolQ.getBackingArray(), GraphuloUtil.EMPTY_BYTES, System.currentTimeMillis());
    Value v = reverse ? multiply(Bval, Aval) : multiply(Aval, Bval);
    return v == null ? Collections.<Entry<Key,Value>>emptyIterator() : Iterators.singletonIterator((Entry<Key, Value>) new SimpleImmutableEntry<>(k, v));
  }

  /** Applies {@link #multiply(Value, Value)} to every pair of consecutive Values with matching
   * row, column family, column qualifier, and column visibility. No hard guarantees on order of operations;
   * makes a best effort to pass the Value that sorts before on the left and the Value that sorts after on the right.
   * Flipped if reversed==true.
   * */
  @Override
  public final Value reduce(Key key, Iterator<Value> iter) {
    if (!iter.hasNext())
      return null;
    Value vPrev = iter.next();
    while (iter.hasNext())
      vPrev = reverse ? multiply(vPrev, iter.next()) : multiply(iter.next(), vPrev);
    return vPrev;
  }

  @Override
  public SimpleTwoScalarOp deepCopy(IteratorEnvironment env) {
    SimpleTwoScalarOp copy = (SimpleTwoScalarOp) super.deepCopy(env);
    copy.fixedValue = new Value(fixedValue);
    copy.reverse = reverse;
    return copy;
  }

  /** Marked final for safety. */
  @Override
  public final void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    seekApplyOp(range, columnFamilies, inclusive);
    super.seek(range, columnFamilies, inclusive);
  }

  /** Marked final for safety. */
  @Override
  public final Key getTopKey() {
    return super.getTopKey();
  }

  /** Marked final for safety. */
  @Override
  public final Value getTopValue() {
    return super.getTopValue();
  }

  /** Marked final for safety. */
  @Override
  public final boolean hasTop() {
    return super.hasTop();
  }

  /** Marked final for safety. */
  @Override
  public final void next() throws IOException {
    super.next();
  }

  /** Marked final for safety. */
  @Override
  protected final void setSource(SortedKeyValueIterator<Key, Value> source) {
    super.setSource(source);
  }

  /** Marked final for safety. */
  @Override
  protected final SortedKeyValueIterator<Key, Value> getSource() {
    return super.getSource();
  }
}
