package edu.mit.ll.graphulo_ocean;

import edu.mit.ll.graphulo.skvi.RemoteSourceIterator;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Take the Cartesian product of a stream of entries with itself.
 * Bundled to consider the vector of kmers for a given sequence all together.
 * <p>
 * The loop looks like this:
 * <pre>
 * for row1 in [tabletStartKey : tabletEndKey]:
 *   for row2 in (row1 : tabletEndKey] from deepCopied skvi:
 *     do BC(row1, row2)
 *   for row2 in (tabletEndKey : -inf) from remote skvi:
 *     do BC(row1, row2)
 * </pre>
 */
public class CartesianDissimilarityIterator implements SortedKeyValueIterator<Key,Value> {

  public static final String OPT_TABLE_PREFIX = "A.";
  public enum DistanceType { BRAY_CURTIS, JACCARD }
  public static final String OPT_DISTANCE_TYPE = "DistanceType";

  /**
   *
   * @param remoteOpts Options to scan the input table. Begin the options with prefix {@link #OPT_TABLE_PREFIX}
   */
  public static IteratorSetting iteratorSetting(int priority, DistanceType distanceType, Map<String,String> remoteOpts) {
    IteratorSetting itset = new IteratorSetting(priority, CartesianDissimilarityIterator.class, remoteOpts);
    itset.addOption(OPT_DISTANCE_TYPE, distanceType.name());
    return itset;
  }

  private DistanceType distanceType;
  private RemoteSourceIterator rsi;
  private SortedKeyValueIterator<Key, Value> source;
  private SortedKeyValueIterator<Key, Value> source2;
  private Map<String,String> origOptions;

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.origOptions = options;
    distanceType = options.containsKey(OPT_DISTANCE_TYPE)
        ? DistanceType.valueOf(options.get(OPT_DISTANCE_TYPE))
        : DistanceType.BRAY_CURTIS;
    this.source = source;
    this.source2 = source.deepCopy(env);
    this.rsi = new RemoteSourceIterator();
    Map<String, String> remoteMap = GraphuloUtil.splitMapPrefix(options).get(
        OPT_TABLE_PREFIX.substring(0,OPT_TABLE_PREFIX.length()-1));
    this.rsi.init(null, remoteMap, env);
  }

  private static class Ret {
    public long sum;
    public Text row;
    public Map<Text,Integer> kmerMap;
  }

  /** Three output arguments. */
  private static Ret buildMapWholeRow(SortedKeyValueIterator<Key, Value> skvi) throws IOException {
    if (!skvi.hasTop())
      return null;
    Ret ret = new Ret();
    Key keyOrig = skvi.getTopKey();
    ret.row = keyOrig.getRow();
//    Text rowOrig = skvi.getTopKey().getRow();
//    Text rowNew = new Text(rowOrig);
    Map<Text,Integer> kmerMap = new HashMap<>();

    long sum = 0;
    while(skvi.hasTop() && skvi.getTopKey().compareRow(ret.row) == 0) {
      int v = Integer.parseInt(new String(skvi.getTopValue().get(), UTF_8));
      sum += v;
      kmerMap.put(skvi.getTopKey().getColumnQualifier(), v);
      skvi.next();
    }
    ret.sum = sum;
    ret.kmerMap = kmerMap;
    return ret;
  }

  private static long sumMap(Map<Text,Integer> m) {
    long sum = 0;
    for (Map.Entry<Text, Integer> e : m.entrySet()) {
      sum += e.getValue();
    }
    return sum;
  }

  /* Todo: This can be optimized to do it on the fly from the second one. */
  private static double brayCurtisDis(Map<Text,Integer> m1, long sum1, Map<Text,Integer> m2, long sum2) {
    Map<Text,Integer> ms, mb;
    if (m1.size() > m2.size()) {
      ms = m2; mb = m1;
    } else {
      ms = m1; mb = m2;
    }
    double sumMin = 0;
    for (Map.Entry<Text, Integer> e : ms.entrySet()) {
      Integer vb = mb.get(e.getKey());
      if (vb != null)
        sumMin += Math.min(vb, e.getValue());
    }
    return 1 - 2 * sumMin / (sum1 + sum2);
  }

  /* Todo: This can be optimized to do it on the fly from the second one. */
  /** THIS METHOD DESTROYS m2. */
  private static double jaccardDis(Map<Text,Integer> m1, Map<Text,Integer> m2) {
    Map<Text,Integer> ms, mb;
    double sumMin = 0, sumMax = 0;
    for (Map.Entry<Text, Integer> e1 : m1.entrySet()) {
      int v1 = e1.getValue();
      Integer v2 = m2.remove(e1.getKey());
      if (v2 != null) {
        // sum the entries in both m1 and m2; max(v1,v2)
        sumMin += Math.min(v2, v1);
        sumMax += Math.max(v2, v1);
      } else {
        // sum the entries in m1 but not in m2; max(v1,0) = v1
        sumMax += v1;
      }
    }
    // sum the entries in m2 but not in e1; max(v2,0) = v2
    for (Map.Entry<Text, Integer> e2 : m2.entrySet()) {
      sumMax += e2.getValue();
    }
    return 1 - sumMin / sumMax;
  }

  private Range seekRange;
  private Collection<ByteSequence> columnFamilies;
  private boolean inclusive;

  @Override
  public boolean hasTop() {
    return nextKey != null;
  }

  @Override
  public void next() throws IOException {
    prepNext();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    this.seekRange = range;
    this.columnFamilies = columnFamilies;
    this.inclusive = inclusive;
    source.seek(range, columnFamilies, inclusive);
    // start range2 on the entry after the first one
    Range rngAfterStart = new Range(range.getStartKey(), false, range.getEndKey(), range.isEndKeyInclusive());
    source2.seek(rngAfterStart, columnFamilies, inclusive);
    rsiFlag = false;

    ret = buildMapWholeRow(source);
    prepNext();
  }

  private Ret ret;
  private Key nextKey;
  private Value nextValue;
  /** Marks when to use rsi vs. source2. */
  private boolean rsiFlag;

  private void prepNext() throws IOException {
    Ret ret2 = buildMapWholeRow(rsiFlag ? rsi : source2);
    // skip self-dissimilarity
    if (ret2 != null && ret2.row.equals(ret.row))
      ret2 = buildMapWholeRow(rsiFlag ? rsi : source2);

    if (ret2 == null && !rsiFlag && !seekRange.isInfiniteStopKey()) {
      // rsi might provide more entries
      rsi.seek(new Range(seekRange.getEndKey(), false, null, false), columnFamilies, inclusive);
      ret2 = buildMapWholeRow(rsi);
      rsiFlag = true;
    }


    if (ret2 == null) {
      // no more entries in rsi or in source2 - advance source and reset source2
      ret = buildMapWholeRow(source);
      if (ret == null) {
        nextKey = null; nextValue = null;
        return;
      }

      // strict upper triangle - seek to the row after the row that source is at.
      source2.seek(seekRange.clip(new Range(ret.row, false, null, false)), columnFamilies, inclusive);
      rsiFlag = false;
      ret2 = buildMapWholeRow(source2);
      if (ret2 == null && !seekRange.isInfiniteStopKey()) {
        // rsi might provide more entries
        rsi.seek(new Range(seekRange.getEndKey(), false, null, false), columnFamilies, inclusive);
        ret2 = buildMapWholeRow(rsi);
        rsiFlag = true;
      }
      if (ret2 == null) {
        nextKey = null; nextValue = null;
        return;
      }
    }

    double dis;
    switch(distanceType) {
      case BRAY_CURTIS:
        dis = brayCurtisDis(ret.kmerMap, ret.sum, ret2.kmerMap, ret2.sum);
        break;
      case JACCARD:
        // DESTROYS ret2.kmerMap
        dis = jaccardDis(ret.kmerMap, ret2.kmerMap);
        break;
      default:
        throw new AssertionError();
    }
    byte[] val = Double.toString(dis).getBytes(UTF_8);
    nextKey = new Key(ret.row, new Text(val), ret2.row);
    nextValue = new Value(val);
  }

  @Override
  public Key getTopKey() {
    return nextKey;
  }

  @Override
  public Value getTopValue() {
    return nextValue;
  }

  @Override
  public CartesianDissimilarityIterator deepCopy(IteratorEnvironment env) {
    CartesianDissimilarityIterator cdi = new CartesianDissimilarityIterator();
    try {
      cdi.init(source.deepCopy(env), origOptions, env);
    } catch (IOException e) {
      throw new RuntimeException("",e);
    }
    return cdi;
  }
}
