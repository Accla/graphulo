package edu.mit.ll.graphulo.skvi.ktruss;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.skvi.MultiKeyCombiner;
import edu.mit.ll.graphulo.util.PeekingIterator2;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.SummingCombiner;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Receives a timestamp threshold <code>tt</code>.
 * This class is an iterator that behaves as follows:
 * <ul>
 *   <li>Entries with ts >= tt are summed together into one entry with the timestamp of the most recent one.</li>
 *   <li>If no entry is seen with ts < tt and we are on scan or full majc scope, then no entries are emitted.</li>
 *   <li>If there are multiple entries with ts < tt, only the most recent is emitted.</li>
 *   <li>All entries are emitted after applying the above rules.</li>
 * </ul>
 * The number of entries emitted are 0, 1, or 2.
 */
public class SumConditionTimestampIterator extends MultiKeyCombiner {

  public static final String TIMESTAMP_THRESHOLD = "tt";

  private long tt;

  public static IteratorSetting iteratorSetting(int priority, long tt) {
    IteratorSetting itset = new IteratorSetting(priority, SumConditionTimestampIterator.class);
    itset.addOption(TIMESTAMP_THRESHOLD, Long.toString(tt));
    Combiner.setCombineAllColumns(itset, true);
    return itset;
  }

  private enum SScope { SCAN_OR_MAJC_FULL, OTHER }
  private SScope sScope;
  private SummingCombiner summer;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    parseOptions(options);
    parseScope(env);
    summer = new SummingCombiner();
    IteratorSetting itset = new IteratorSetting(1, SummingCombiner.class);
    SummingCombiner.setCombineAllColumns(itset, true);
    SummingCombiner.setEncodingType(itset, LongCombiner.Type.STRING);
    summer.init(null, itset.getOptions(), env);
  }

  private void parseScope(IteratorEnvironment env) {
    switch (env.getIteratorScope()) {
      case scan: sScope = SScope.SCAN_OR_MAJC_FULL; break;
      case minc: sScope = SScope.OTHER; break;
      case majc:
        if (env.isFullMajorCompaction())
          sScope = SScope.SCAN_OR_MAJC_FULL;
        else
          sScope = SScope.OTHER;
        break;
      default: throw new AssertionError();
    }
  }

  private void parseOptions(Map<String, String> options) {
    tt = Long.parseLong(options.get(TIMESTAMP_THRESHOLD));
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> reduceKV(Iterator<Map.Entry<Key, Value>> iter) {
    // If there are multiple entries with ts < tt, only the most recent is emitted.
    Key keyBeforeTT = null, keyAfterTT = null;
    Value valueBeforeTT = null;
    List<Value> valuesAfterTT = new LinkedList<>();

    while (iter.hasNext()) {
      Map.Entry<Key, Value> next = iter.next();
      Key k = next.getKey();
      Value v = next.getValue();

      if (k.getTimestamp() < tt) {
        if (keyBeforeTT == null || k.getTimestamp() > keyBeforeTT.getTimestamp()) {
          keyBeforeTT = k;
          valueBeforeTT = v;
        }
      } else {
        if (keyAfterTT == null || k.getTimestamp() > keyAfterTT.getTimestamp())
          keyAfterTT = k;
        valuesAfterTT.add(v);
      }
    }

    // Entries with ts >= tt are summed together into one entry with the timestamp of the most recent one.
    // If no entry is seen with ts < tt and we are on scan or full majc scope, then no entries are emitted.
    if (sScope == SScope.SCAN_OR_MAJC_FULL && keyBeforeTT == null)
      return null;

    // All entries are emitted after applying the above rules.
    if (keyBeforeTT == null && keyAfterTT == null)
      return null;
    else if (keyAfterTT == null)
      return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(keyBeforeTT, valueBeforeTT));
    else if (keyBeforeTT == null)
      return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(keyAfterTT, summer.reduce(keyAfterTT, valuesAfterTT.iterator())));
    else {
      // emit most recent entry first
      return new PeekingIterator2<Map.Entry<Key, Value>>(
          new AbstractMap.SimpleImmutableEntry<>(keyAfterTT, summer.reduce(keyAfterTT, valuesAfterTT.iterator())),
          new AbstractMap.SimpleImmutableEntry<>(keyBeforeTT, valueBeforeTT)
      );
    }
  }

  @Override
  public SumConditionTimestampIterator deepCopy(IteratorEnvironment env) {
    SumConditionTimestampIterator n = (SumConditionTimestampIterator)super.deepCopy(env);
    n.tt = this.tt;
    n.summer = this.summer; // no need to deepCopy summer
    return n;
  }
}
