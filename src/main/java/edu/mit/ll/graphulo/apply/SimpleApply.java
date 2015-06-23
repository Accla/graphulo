package edu.mit.ll.graphulo.apply;

import com.google.common.collect.Iterators;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * A abstract class for simple element0-wise function application
 * that returns zero or one entry per apply.
 */
public abstract class SimpleApply implements ApplyOp {
  /** Implements simple apply logic. Returning null means no entry is emitted. */
  public abstract Value simpleApply(Key k, Value v);

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(final Key k, Value v1) {
//    Key k = new Key(Mrow.getBackingArray(), ATcolF.getBackingArray(),
//        ATcolQ.getBackingArray(), GraphuloUtil.EMPTY_BYTES, System.currentTimeMillis());
    Value v2 = simpleApply(k, v1);
    return v2 == null ? Collections.<Map.Entry<Key,Value>>emptyIterator() : Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, v2));
  }
}
