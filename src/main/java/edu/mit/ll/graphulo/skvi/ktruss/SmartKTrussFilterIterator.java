package edu.mit.ll.graphulo.skvi.ktruss;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

import edu.mit.ll.graphulo.apply.ApplyOp;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.google.common.collect.Iterators;

import edu.mit.ll.graphulo.skvi.MultiKeyCombiner;
import edu.mit.ll.graphulo.util.PeekingIterator2;
import org.apache.accumulo.core.iterators.user.SummingCombiner;

/**
 * A filter that accepts values that are (1) odd and (2) have (val - 1)/2 >= k-2, where k is a parameter.
 * Used by the kTrussAdj algorithm.
 */
public class SmartKTrussFilterIterator extends Filter {

  public static final String K = "k";

  private int k;

  public static IteratorSetting iteratorSetting(int priority, int k) {
    IteratorSetting itset = new IteratorSetting(priority, SmartKTrussFilterIterator.class);
    itset.addOption(K, Integer.toString(k));
    return itset;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    parseOptions(options);
  }

  private void parseOptions(Map<String, String> options) {
    k = Integer.parseInt(options.get(K));
  }

  @Override
  public boolean accept(Key key, Value v) {
    int l = Integer.parseInt(v.toString());
    return l % 2 == 1 && (l-1)/2 >= k-2;
  }

  @Override
  public SmartKTrussFilterIterator deepCopy(IteratorEnvironment env) {
    SmartKTrussFilterIterator n = (SmartKTrussFilterIterator)super.deepCopy(env);
    n.k = k;
    return n;
  }
}
