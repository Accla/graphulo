package edu.mit.ll.graphulo.apply;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Only retain the part of the Key given as a PartialKey option.
 * If null, then reduces the Key to the seek start Key (which is the all empty fields Key if seek range starts at -inf).
 */
public class KeyRetainOnlyApply implements ApplyOp {
  private static final Logger log = LogManager.getLogger(KeyRetainOnlyApply.class);

  public static final String PARTIAL_KEY = "PartialKey";

  /** Create an IteratorSetting that prunes every Key it sees.
   * A null <tt>pk</tt> means reduce the Key to the seek start Key (which is the all empty fields Key if seek range starts at -inf). */
  public static IteratorSetting iteratorSetting(int priority, PartialKey pk) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, KeyRetainOnlyApply.class.getName());
    if (pk != null)
      itset.addOption(ApplyIterator.APPLYOP+ApplyIterator.OPT_SUFFIX+PARTIAL_KEY, pk.name());
    return itset;
  }

  private PartialKey pk = null;
  private Key seekStartKey = null;

  private void parseOptions(Map<String,String> options) {
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String v = entry.getValue();
      // can replace with switch in Java 1.7
      String s = entry.getKey();
      if (s.equals(PARTIAL_KEY)) {
        if (v.isEmpty())
          pk = null;
        else
          pk = PartialKey.valueOf(v);

      } else {
        log.warn("Unrecognized option: " + entry);

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
      knew = seekStartKey;
    else
      knew = GraphuloUtil.keyCopy(k, pk);
    return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<Key, Value>(knew, v));
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    if (pk == null) {
      if (range.isInfiniteStartKey())
        seekStartKey = new Key();
      else {
        if (range.isStartKeyInclusive())
          seekStartKey = range.getStartKey(); // not necessarily empty column family/qualifier
        else
          seekStartKey = range.getStartKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL);
      }
    }
  }
}
