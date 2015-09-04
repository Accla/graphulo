package edu.mit.ll.graphulo.skvi;

import org.apache.accumulo.core.client.IteratorSetting;
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

  public static final String FILTER = "filter", KEYPART = "keyPart";

  public static IteratorSetting iteratorSetting(int priority, KeyPart keyPart, String filter) {
    IteratorSetting itset = new IteratorSetting(priority, D4mRangeFilter.class);
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
  private SortedSet<Range> filter = new TreeSet<>(Collections.singleton(new Range()));

  @Override
  public boolean accept(Key key, Value value) {
    Key data;
    switch (keyPart) {
      case ROW:
        data = new Key(key.getRow());
        break;
      case COLF:
        data = new Key(key.getColumnFamily());
        break;
      case COLQ:
        data = new Key(key.getColumnQualifier());
        break;
      case VAL:
        data = new Key(value.toString());
        break;
      default: throw new AssertionError();
    }
//    Range r = new Range(data);
    Iterator<Range> iter = filter.iterator();
    boolean ok = false;
    while (!ok && iter.hasNext())
      ok = iter.next().contains(data);
    return ok;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (options.containsKey(FILTER))
      filter = RemoteSourceIterator.parseRanges(options.get(FILTER));
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
