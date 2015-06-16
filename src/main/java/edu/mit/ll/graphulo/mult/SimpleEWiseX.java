package edu.mit.ll.graphulo.mult;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

/**
 * A simple abstract class for element0-wise multiplication
 * that returns zero or one entry per multiply.
 */
public abstract class SimpleEWiseX implements IMultiplyOp {

  /** Implements simple multiply logic. Returning null means no entry is emitted. */
  public abstract Value multiply(Value Aval, Value Bval);

  private Entry<Key,Value> kv;

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
  }

  @Override
  public void startRow(SortedMap<Key, Value> mapRowA, SortedMap<Key, Value> mapRowB) {
  }

  @Override
  public void multiply(ByteSequence Mrow, ByteSequence ATcolF, ByteSequence ATcolQ, ByteSequence BcolF, ByteSequence BcolQ, Value ATval, Value Bval) {
    Key k = new Key(Mrow.getBackingArray(), ATcolF.getBackingArray(),
        ATcolQ.getBackingArray(), EMPTY_BYTES, System.currentTimeMillis());
    Value v = multiply(ATval, Bval);
    kv = v == null ? null : new AbstractMap.SimpleImmutableEntry<>(k,v);
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
