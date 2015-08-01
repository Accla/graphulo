package edu.mit.ll.graphulo.simplemult;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.ewise.EWiseOp;
import edu.mit.ll.graphulo.reducer.Reducer;
import edu.mit.ll.graphulo.rowmult.MultiplyOp;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;


/**
 * A simple abstract class for an operation acting on two scalars.
 * This includes matrix multiplication, element-wise multiplication,
 * element-wise sum, Combiner-style sum, unary Apply, and Reductions.
 * <p>
 * The requirement is that this operation logic must return zero or one entry per multiply
 * and depend only on Values, not Keys.  More advanced operations should not use this class.
 * Certain methods are marked as final to prevent misuse/confusion.
 * Please take care that your operator is commutative for the Combiner and Reduce usages.
 * Your operator should always be associative.
 * Finally, in the case of EWiseOp, the operation logic must not need to act on non-matching Values.
 * <p>
 *   Can be used for unary Apply if passed a fixed Value operand
 *   on the left or right of the multiplication inside iterator options.
 *   Defaults to the left side of the multiplication; pass {@value #REVERSE} if the right is desired.
 */
public abstract class SimpleTwoScalar extends KeyTwoScalar implements MultiplyOp, EWiseOp, Reducer {
  private static final Logger log = LogManager.getLogger(SimpleTwoScalar.class);

  //////////////////////////////////////////////////////////////////////////////////
  /** Implements simple multiply logic. Returning null means no entry is emitted. */
  public abstract Value multiply(Value Aval, Value Bval);
  //////////////////////////////////////////////////////////////////////////////////

  // MultiplyOp
  @Override
  public final Iterator<? extends Entry<Key, Value>> multiply(ByteSequence Mrow, ByteSequence ATcolF, ByteSequence ATcolQ, ByteSequence BcolF, ByteSequence BcolQ, Value ATval, Value Bval) {
//    System.err.println("Mrow:"+Mrow+" ATcolQ:"+ATcolQ+" BcolQ:"+BcolQ+" ATval:"+ATval+" Bval:"+Bval);
    assert ATval != null || Bval != null;
    Key k = new Key(ATcolQ.getBackingArray(), ATcolF.getBackingArray(),
        BcolQ.getBackingArray(), GraphuloUtil.EMPTY_BYTES, System.currentTimeMillis());
    Value v = reverse ? multiply(Bval, ATval) : multiply(ATval, Bval);
    return v == null ? Collections.<Entry<Key,Value>>emptyIterator() : Iterators.singletonIterator((Entry<Key, Value>) new SimpleImmutableEntry<>(k, v));
  }

  private static final byte[] EMPTY_BYTES = new byte[0];

  // EWiseOp
  @Override
  public final Iterator<? extends Entry<Key, Value>> multiply(ByteSequence Mrow, ByteSequence McolF, ByteSequence McolQ, Value Aval, Value Bval) {
    // Important!  Aval xor Bval could be null, if emitNoMatchA or emitNoMatchB are true in TwoTableIterator.
    // Decision is to emit the non-matching entries untouched by the operation.  This is a *SIMPLETwoScalar* operator.
    assert Aval != null || Bval != null;
    if (Aval == null)
      return Iterators.singletonIterator((Entry<Key, Value>) new SimpleImmutableEntry<>(
          new Key(Mrow.getBackingArray(), McolF.getBackingArray(), McolQ.getBackingArray(), EMPTY_BYTES, System.currentTimeMillis()),
          Bval));
    if (Bval == null)
      return Iterators.singletonIterator((Entry<Key, Value>) new SimpleImmutableEntry<>(
          new Key(Mrow.getBackingArray(), McolF.getBackingArray(), McolQ.getBackingArray(), EMPTY_BYTES, System.currentTimeMillis()),
          Aval));

    Key k = new Key(Mrow.getBackingArray(), McolF.getBackingArray(),
        McolQ.getBackingArray(), GraphuloUtil.EMPTY_BYTES, System.currentTimeMillis());
    Value v = reverse ? multiply(Bval, Aval) : multiply(Aval, Bval);
    return v == null ? Collections.<Entry<Key,Value>>emptyIterator() : Iterators.singletonIterator((Entry<Key, Value>) new SimpleImmutableEntry<>(k, v));
  }

  @Override
  public final Value multiply(Key key, Value v1, Value v2) {
    return multiply(v1, v2);
  }


  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("SimpleTwoScalar");
    io.setDescription("A Combiner that operates on every pair of entries matching row through column visibility, that does not need to see the Key");
    return io;
  }

  private Value reducerV;

  @Override
  public final void reset() {
    reducerV = null;
  }

  @Override
  public final void update(Key k, Value v) {
    if (reducerV == null)
      reducerV = new Value(v);
    else
      reducerV = reverse ? multiply(reducerV, v) : multiply(v, reducerV);
  }

  @Override
  public final void combine(byte[] another) {
    Value v = new Value(another);
    if (reducerV == null)
      reducerV = new Value(v);
    else
      reducerV = reverse ? multiply(reducerV, v) : multiply(v, reducerV);
  }

  @Override
  public final boolean hasTopForClient() {
    return reducerV != null;
  }

  @Override
  public final byte[] getForClient() {
    return reducerV.get();
  }

}
