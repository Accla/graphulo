package edu.mit.ll.graphulo.skvi;

import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.RangeSet;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Only emit entries whose column appears in the given collection of ranges.
 */
public class D4mRangeFilter extends Filter {

  public enum KeyPart { ROW, COLF, COLQ, VAL}

  public static ByteSequence getKeyBytes(Key k, Value v, KeyPart kp) {
    switch (kp) {
      case ROW:
        return k.getRowData();
      case COLF:
        return k.getColumnFamilyData();
      case COLQ:
        return k.getColumnQualifierData();
      case VAL:
        return new ArrayByteSequence(v.get());
      default: throw new AssertionError();
    }
  }

  public static final String FILTER = "filter", KEYPART = "keyPart";

  public static IteratorSetting iteratorSetting(int priority, KeyPart keyPart, String filter) {
    IteratorSetting itset = new IteratorSetting(priority, D4mRangeFilter.class);
    if (keyPart != null)
      itset.addOption(KEYPART, keyPart.name());
    itset.addOption(FILTER, filter);
    return itset;
  }

  public static IteratorSetting iteratorSetting(int priority, KeyPart keyPart, String filter, boolean negate) {
    IteratorSetting itset = iteratorSetting(priority, keyPart, filter);
    itset.addOption(NEGATE, Boolean.toString(negate));
    return itset;
  }

  private KeyPart keyPart = KeyPart.ROW;
  private RangeSet<ByteSequence> filter = ImmutableRangeSet.of(com.google.common.collect.Range.<ByteSequence>all());

  @Override
  public boolean accept(Key key, Value value) {
    ByteSequence bs = getKeyBytes(key, value, keyPart);
    return filter.contains(bs);
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (options.containsKey(FILTER))
      filter = GraphuloUtil.d4mRowToGuavaRangeSet(options.get(FILTER), false);
    if (options.containsKey(KEYPART))
      keyPart = KeyPart.valueOf(options.get(KEYPART));
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("D4mRangeFilter");
    io.setDescription("Filter based on ranges of strings in D4M syntax");
    KeyPart[] values = KeyPart.values();
    String[] strs = new String[values.length];
    for (int i = 0; i < values.length; i++) {
      strs[i] = values[i].name();
    }
    io.addNamedOption(KEYPART, "Which part of key to filter [" + Arrays.toString(strs) + "] [default " + KeyPart.COLQ.name()+"]");
    io.addNamedOption(FILTER, "Column ranges to scan for remote Accumulo table, Matlab syntax. (default ':,' all)");
    return io;
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    D4mRangeFilter result = (D4mRangeFilter) super.deepCopy(env);
    result.filter = filter;
    result.keyPart = keyPart;
    return result;
  }
}
