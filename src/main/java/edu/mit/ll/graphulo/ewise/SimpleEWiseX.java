package edu.mit.ll.graphulo.ewise;

import edu.mit.ll.graphulo.ewise.EWiseOp;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A simple abstract class for element0-wise multiplication
 * that returns zero or one entry per multiply.
 */
public abstract class SimpleEWiseX implements EWiseOp, Iterator<Entry<Key,Value>> {

  /** Implements simple multiply logic. Returning null means no entry is emitted. */
  public abstract Value multiply(Value Aval, Value Bval);

  private Entry<Key,Value> kv;

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
  }

  @Override
  public Iterator<? extends Entry<Key, Value>> multiply(ByteSequence Mrow, ByteSequence ATcolF, ByteSequence ATcolQ, Value ATval, Value Bval) {
    Key k = new Key(Mrow.getBackingArray(), ATcolF.getBackingArray(),
        ATcolQ.getBackingArray(), GraphuloUtil.EMPTY_BYTES, System.currentTimeMillis());
    Value v = multiply(ATval, Bval);
    kv = v == null ? null : new AbstractMap.SimpleImmutableEntry<>(k,v);
    return this;
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