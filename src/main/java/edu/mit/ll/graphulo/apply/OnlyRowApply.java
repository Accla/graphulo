package edu.mit.ll.graphulo.apply;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Drop the column family and qualifier from Keys. Only retains Row and Value.
 */
public class OnlyRowApply implements ApplyOp {
  private static final Logger log = LogManager.getLogger(OnlyRowApply.class);

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v) {
    log.info("transform "+k+" -> "+v+" => "+GraphuloUtil.keyCopy(k, PartialKey.ROW));
    return Iterators.singletonIterator(
        new AbstractMap.SimpleImmutableEntry<>(
            GraphuloUtil.keyCopy(k, PartialKey.ROW), v
        )
    );
  }
}
