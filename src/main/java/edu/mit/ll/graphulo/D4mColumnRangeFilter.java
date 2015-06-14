package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Only emit entries whose column appears in the given collection of ranges.
 */
public class D4mColumnRangeFilter extends Filter {

  public static final String COLRANGES = "colRanges";

  private SortedSet<Range> colRanges = new TreeSet<>(Collections.singleton(new Range()));

  @Override
  public boolean accept(Key key, Value value) {
    Key colQ = new Key(key.getColumnQualifier());
//    Range r = new Range(colQ);
    Iterator<Range> iter = colRanges.iterator();
    boolean ok = false;
    while (!ok && iter.hasNext())
      ok = iter.next().contains(colQ);
    return ok;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (options.containsKey(COLRANGES))
      colRanges = RemoteSourceIterator.parseRanges(options.get(COLRANGES));
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("D4mColumnRangeFilter");
    io.setDescription("Filter for key/value pairs based on a ranges of column qualifier names in D4M syntax");
    io.addNamedOption(COLRANGES, "Column ranges to scan for remote Accumulo table, Matlab syntax. (default ':,' all)");
    return io;
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    D4mColumnRangeFilter result = (D4mColumnRangeFilter) super.deepCopy(env);
    result.colRanges = colRanges;
    return result;
  }
}
