package edu.mit.ll.graphulo.skvi.ktruss;

import com.sun.xml.internal.xsom.impl.scd.Iterators;
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
 *   <li>If no entry is seen with ts < tt, then no entries are emitted.</li>
 *   <li>If there is an entry with ts < tt, then the most recent entry with ts < tt
 *   as well as the sum of all entries with ts > tt are emitted.</li>
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

  private SummingCombiner summer;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    parseOptions(options);

    summer = new SummingCombiner();
    IteratorSetting itset = new IteratorSetting(1, SummingCombiner.class);
    SummingCombiner.setCombineAllColumns(itset, true);
    SummingCombiner.setEncodingType(itset, LongCombiner.Type.STRING);
    summer.init(null, itset.getOptions(), env);
  }

  private void parseOptions(Map<String, String> options) {
    tt = Long.parseLong(options.get(TIMESTAMP_THRESHOLD));
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> reduceKV(Iterator<Map.Entry<Key, Value>> iter) {
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

    if (keyBeforeTT == null)
      return null;
    if (keyAfterTT == null) {
      return Iterators.singleton(new AbstractMap.SimpleImmutableEntry<>(keyBeforeTT, valueBeforeTT));
    } else {
      // emit most recent entry first
      return new PeekingIterator2<Map.Entry<Key, Value>>(
          new AbstractMap.SimpleImmutableEntry<>(keyAfterTT, summer.reduce(keyAfterTT, valuesAfterTT.iterator())),
          new AbstractMap.SimpleImmutableEntry<>(keyBeforeTT, valueBeforeTT)
      );
    }
  }
}
