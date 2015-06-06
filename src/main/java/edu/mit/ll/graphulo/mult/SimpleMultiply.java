package edu.mit.ll.graphulo.mult;

import edu.mit.ll.graphulo.IMultiplyOp;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;

/**
 * A simple abstract class for matrix multiplication
 * that returns zero or one entry per multiply.
 */
public abstract class SimpleMultiply implements IMultiplyOp {

  /** Implements simple multiply logic. Returning null means no entry is emitted. */
  public abstract Value multiply(Value ATval, Value Bval);

  private Entry<Key,Value> kv;

  @Override
  public void multiply(ByteSequence Mrow, ByteSequence ATcolF, ByteSequence ATcolQ, ByteSequence BcolF, ByteSequence BcolQ, Value ATval, Value Bval) {
    Key k = new Key(ATcolQ.getBackingArray(), ATcolF.getBackingArray(),
        BcolQ.getBackingArray(), EMPTY_BYTES, System.currentTimeMillis());
    Value v = multiply(ATval, Bval);
    kv = v == null ? null : new SimpleImmutableEntry<>(k,v);
  }

  @Override
  public boolean hasNext() {
    return kv != null;
  }

  @Override
  public Entry<Key,Value> next() {
    Entry<Key,Value> ret = kv;
    kv = null;
    return ret;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
