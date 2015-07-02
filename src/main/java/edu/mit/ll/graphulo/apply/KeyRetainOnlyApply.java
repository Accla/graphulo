package edu.mit.ll.graphulo.apply;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
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
 * Only retain the part of the Key given as a PartialKey option.
 * If null, then reduces the Key to the very first Key with all empty fields.
 */
public class KeyRetainOnlyApply implements ApplyOp {
  private static final Logger log = LogManager.getLogger(KeyRetainOnlyApply.class);

  public static final String PARTIAL_KEY = "PartialKey";

  /** Create an IteratorSetting that prunes every Key it sees.
   * A null <tt>pk</tt> means reduce the Key to the very first Key with all empty fields. */
  public static IteratorSetting iteratorSetting(int priority, PartialKey pk) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, KeyRetainOnlyApply.class.getName());
    if (pk != null)
      itset.addOption(ApplyIterator.APPLYOP+ApplyIterator.OPT_SUFFIX+PARTIAL_KEY, pk.name());
    return itset;
  }

  private PartialKey pk = null;

  private void parseOptions(Map<String,String> options) {
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String v = entry.getValue();
      switch (entry.getKey()) {
        case PARTIAL_KEY:
          if (v.isEmpty())
            pk = null;
          else
            pk = PartialKey.valueOf(v);
          break;
        default:
          log.warn("Unrecognized option: " + entry);
          break;
      }
    }
  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    parseOptions(options);
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v) {
    Key knew;
    if (pk == null)
      knew = new Key();
    else
      knew = GraphuloUtil.keyCopy(k, pk);
    return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(knew, v));
  }
}
