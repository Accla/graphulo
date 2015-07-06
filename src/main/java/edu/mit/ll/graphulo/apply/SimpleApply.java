package edu.mit.ll.graphulo.apply;

import com.google.common.collect.Iterators;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * A abstract class for simple element-wise function application
 * that returns zero or one entry per apply.
 */
public abstract class SimpleApply implements ApplyOp {
  private static final Logger log = LogManager.getLogger(SimpleApply.class);

  /** Implements simple apply logic. Returning null means no entry is emitted. */
  public abstract Value simpleApply(Key k, Value v);

  /** Unrecognized options printed here. */
  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    for (Map.Entry<String, String> entry : options.entrySet()) {
      log.warn("Unrecognized option: "+entry.getKey()+" -> "+entry.getValue());
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(final Key k, Value v1) {
//    Key k = new Key(Mrow.getBackingArray(), ATcolF.getBackingArray(),
//        ATcolQ.getBackingArray(), GraphuloUtil.EMPTY_BYTES, System.currentTimeMillis());
    Value v2 = simpleApply(k, v1);
    return v2 == null ? Collections.<Map.Entry<Key,Value>>emptyIterator() : Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, v2));
  }
}
