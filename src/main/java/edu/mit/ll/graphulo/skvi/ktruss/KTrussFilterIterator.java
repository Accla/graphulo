package edu.mit.ll.graphulo.skvi.ktruss;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.SummingCombiner;

import com.sun.xml.internal.xsom.impl.scd.Iterators;

import edu.mit.ll.graphulo.skvi.MultiKeyCombiner;
import edu.mit.ll.graphulo.util.PeekingIterator2;

/**
 * Receives a timestamp threshold <code>k</code>.
 * This class is a MultiKeyCombiner that behaves as follows:
 * <ol>
 *   <li>If not exactly two entries are present, no entries are emitted.</li>
 *   <li>If the entry with the greater timestamp has a value < k-2, no entries are emitted.</li>
 *   <li>Otherwise emits both entries unchanged.</li>
 * </ol>
 * This iterator should only be used after all entries are written and only on scan and full major compactions.
 * During scans, this iterator does two additional tasks:
 * <ol>
 *   <li>Does not emit the entry with an earlier (less recent) timestamp.</li>
 *   <li>Change the value of the entry with the more recent timestamp to "1".</li>
 * </ol>
 */
public class KTrussFilterIterator extends MultiKeyCombiner {

  public static final String K = "k";

  private int k;

  public static IteratorSetting iteratorSetting(int priority, int k) {
    IteratorSetting itset = new IteratorSetting(priority, KTrussFilterIterator.class);
    itset.addOption(K, Integer.toString(k));
    Combiner.setCombineAllColumns(itset, true);
    return itset;
  }

  private enum KScope { SCAN, MAJC_FULL, DISABLE }
  private KScope kScope;
  private static final Value VALUE_ONE = new Value("1".getBytes());

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);

    switch (env.getIteratorScope()) {
      case scan: kScope = KScope.SCAN; break;
      case minc: kScope = KScope.DISABLE; break;
      case majc:
        if (env.isFullMajorCompaction())
          kScope = KScope.MAJC_FULL;
        else
          kScope = KScope.DISABLE;
        break;
      default: throw new AssertionError();
    }

    parseOptions(options);
  }

  private void parseOptions(Map<String, String> options) {
    k = Integer.parseInt(options.get(K));
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> reduceKV(Iterator<Map.Entry<Key, Value>> iter) {
    if (kScope == KScope.DISABLE)
      return iter;

    Map.Entry<Key,Value> eBefore, eAfter;
    {
      Map.Entry<Key,Value> e1, e2;
      if (!iter.hasNext())
        return null;
      e1 = iter.next();
      if (!iter.hasNext())
        return null;
      e2 = iter.next();
      if (e1.getKey().getTimestamp() < e2.getKey().getTimestamp()) {
        eBefore = e1; eAfter = e2;
      } else {
        eBefore = e2; eAfter = e1;
      }
    }

    if (Integer.parseInt(eAfter.getValue().toString()) < k-2)
      return null;
    if (kScope == KScope.MAJC_FULL)
      return new PeekingIterator2<>(eAfter, eBefore);

    assert kScope == KScope.SCAN;
    return Iterators.singleton(new AbstractMap.SimpleImmutableEntry<>(eAfter.getKey(), VALUE_ONE));
  }
}
