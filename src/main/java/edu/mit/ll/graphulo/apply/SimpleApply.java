package edu.mit.ll.graphulo.apply;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A abstract class for simple element0-wise function application
 * that returns zero or one entry per apply.
 */
public abstract class SimpleApply implements ApplyOp, Iterator<Map.Entry<Key,Value>> {
  /** Implements simple apply logic. Returning null means no entry is emitted. */
  public abstract Value simpleApply(Key k, Value v);

  private Map.Entry<Key,Value> kv;

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(final Key k, Value v) {
//    Key k = new Key(Mrow.getBackingArray(), ATcolF.getBackingArray(),
//        ATcolQ.getBackingArray(), GraphuloUtil.EMPTY_BYTES, System.currentTimeMillis());
    Value v2 = simpleApply(k, v);
    kv = v2 == null ? null : new AbstractMap.SimpleImmutableEntry<>(k,v2);
    return this;
  }

  @Override
  public boolean hasNext() {
    return kv != null;
  }

  @Override
  public Map.Entry<Key,Value> next() {
    Map.Entry<Key,Value> ret = kv;
    kv = null;
    return ret;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
