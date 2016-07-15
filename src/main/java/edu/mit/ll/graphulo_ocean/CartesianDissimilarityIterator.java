package edu.mit.ll.graphulo_ocean;

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
 * Problem: will not scale to >1 tablet server
 */
public class CartesianDissimilarityIterator implements SortedKeyValueIterator<Key,Value> {

  public static IteratorSetting iteratorSetting(int priority) {
    IteratorSetting itset = new IteratorSetting(priority, CartesianDissimilarityIterator.class);
    return itset;
  }

  private SortedKeyValueIterator<Key, Value> source;
  private SortedKeyValueIterator<Key, Value> source2;

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
    this.source2 = source.deepCopy(env);
  }

  private static class Ret {
    public long sum;
    public Text row;
    public Map<Text,Integer> kmerMap;
  }

  /** Three output arguments */
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
    source2.seek(range, columnFamilies, inclusive);
//    if (!source.hasTop() || !source2.hasTop())
    ret = buildMapWholeRow(source);
    prepNext();
  }

  private Ret ret;
  private Key nextKey;
  private Value nextValue;

  private void prepNext() throws IOException {
    Ret ret2 = buildMapWholeRow(source2);
    // skip self-dissimilarity
    if (ret2 != null && ret2.row.equals(ret.row))
      ret2 = buildMapWholeRow(source2);

    if (ret2 == null) {
      ret = buildMapWholeRow(source);
      if (ret == null) {
        nextKey = null; nextValue = null;
        return;
      }

      // strict upper triangle - seek to the row after the row that source is at.
      source2.seek(seekRange.clip(new Range(ret.row, false, null, false)), columnFamilies, inclusive);

      ret2 = buildMapWholeRow(source2);
      if (ret2 == null) {
        nextKey = null; nextValue = null;
        return;
      }
    }

    double dis = brayCurtisDis(ret.kmerMap, ret.sum, ret2.kmerMap, ret2.sum);
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
    cdi.source = source.deepCopy(env);
    cdi.source2 = source.deepCopy(env);
    return cdi;
  }
}
