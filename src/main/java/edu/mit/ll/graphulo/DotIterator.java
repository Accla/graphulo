package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.mult.BigDecimalMultiply;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Outer product. Emits partial products. Configure two remote sources for tables AT and B,
 * or configure one remote source and use the source as the other one.
 */
public class DotIterator implements SortedKeyValueIterator<Key, Value>, OptionDescriber {
  private static final Logger log = LogManager.getLogger(DotIterator.class);

  private IMultiplyOp multiplyOp = new BigDecimalMultiply();
  private SortedKeyValueIterator<Key,Value> remoteAT, remoteB;

  private PeekingIterator<Map.Entry<Key, Value>> bottomIter; // = new TreeMap<>(new ColFamilyQualifierComparator());

  public static final String PREFIX_AT = "AT";
  public static final String PREFIX_B = "B";
  private static final Range INFINITE_RANGE = new Range();

  static final OptionDescriber.IteratorOptions iteratorOptions;

  static {
    final Map<String, String> optDesc = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
      optDesc.put(PREFIX_AT + entry.getKey(), "Table AT:" + entry.getValue());
    }
    for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
      optDesc.put(PREFIX_B + entry.getKey(), "Table B:" + entry.getValue());
    }

    iteratorOptions = new OptionDescriber.IteratorOptions("DotMultIterator",
        "Outer product on A and B, given AT and B. Can omit one and use parent source instead. Does not sum.",
        optDesc, null);
  }

  @Override
  public IteratorOptions describeOptions() {
    return iteratorOptions;
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    return validateOptionsStatic(options);
  }

  public static boolean validateOptionsStatic(Map<String, String> options) {
    Map<String, String> optAT = new HashMap<>(), optB = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(PREFIX_AT))
        optAT.put(key.substring(PREFIX_AT.length()), entry.getValue());
      else if (key.startsWith(PREFIX_B))
        optB.put(key.substring(PREFIX_B.length()), entry.getValue());
      else switch (key) {

          default:
            throw new IllegalArgumentException("unknown option: " + entry);
        }
    }
    return (!optAT.isEmpty() || !optB.isEmpty()) &&
        (optAT.isEmpty() || RemoteSourceIterator.validateOptionsStatic(optAT)) &&
        (optB.isEmpty() || RemoteSourceIterator.validateOptionsStatic(optB));
  }


  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    // parse options, pass correct options to RemoteSourceIterator init()
    Map<String, String> optAT = null, optB = null;
    {
      Map<String, Map<String, String>> prefixMap = GraphuloUtil.splitMapPrefix(options);
      for (Map.Entry<String, Map<String, String>> prefixEntry : prefixMap.entrySet()) {
        String prefix = prefixEntry.getKey();
        Map<String, String> entryMap = prefixEntry.getValue();
        switch (prefix) {
          case PREFIX_AT: {
            String v = entryMap.remove("doWholeRow");
            if (v != null && !Boolean.parseBoolean(v)) {
              log.warn("Forcing doWholeRow option on table A to FALSE. Given: " + v);
            }
            optAT = entryMap;
            optAT.put("doWholeRow", "false");
            break;
          }
          case PREFIX_B: {
            String v = entryMap.remove("doWholeRow");
            if (v != null && !Boolean.parseBoolean(v)) {
              log.warn("Forcing doWholeRow option on table BT to FALSE. Given: " + v);
            }
            optB = entryMap;
            optB.put("doWholeRow", "false");
            break;
          }
          default:
            for (Map.Entry<String, String> entry : entryMap.entrySet()) {
              log.warn("Unrecognized option: " + prefix + '.' + entry);
            }
            break;
        }
      }
    }

    if ((optAT == null && optB == null) ||
        (optAT == null || optB == null) && source == null) {
      throw new IllegalArgumentException("not enough options given: optAT="+optAT+", optB="+optB+", source="+source);
    }
    if (optAT != null && optB != null && source != null) {
      log.warn("DotMultIterator ignores/replaces parent source passed in init(): " + source);
    }

    if (optAT != null) {
      remoteAT = new RemoteSourceIterator();
      remoteAT.init(null, optAT, env);
    } else
      remoteAT = source;
    if (optB != null) {
      remoteB = new RemoteSourceIterator();
      remoteB.init(null, optB, env);
    } else
      remoteB = source;
  }


  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    Range rA = INFINITE_RANGE, rBT = INFINITE_RANGE;
    // if range is in the middle of a row, seek A to the beginning of the row and B to the correct location
//    if (range.isInfiniteStartKey()) {
//      rBT = INFINITE_RANGE;
//      if (range.isInfiniteStopKey())
//        rA = INFINITE_RANGE;
//      else
//        rA = new Range(null, range.getEndKey().getRow());
//    } else {
//      rBT = new Range(range.getStartKey().getColumnQualifier(), range.isStartKeyInclusive(), null, false);
//      // Use special start key to work around seek() method of RowEncodingIterator
//      Key startK = new Key(range.getStartKey().getRow());
//      if (!range.isStartKeyInclusive())
//        startK.setTimestamp(Long.MAX_VALUE - 1);
//      if (range.isInfiniteStopKey())
//        rA = new Range(startK, range.isStartKeyInclusive(), null, false);
//      else
//        rA = new Range(startK, range.isStartKeyInclusive(), new Key(range.getEndKey().getRow()), true);
//    }
    log.debug("seek range: " + range);
    log.debug("rA   range: " + rA);
    log.debug("rBT  range: " + rBT);

    Watch.instance.start(Watch.PerfSpan.ATnext);
    try {
      remoteAT.seek(rA, Collections.<ByteSequence>emptySet(), false);
    } finally {
      Watch.instance.stop(Watch.PerfSpan.ATnext);
    }
    // choosing not to handle case where we end in the middle of a row; allowed to return entries beyond the seek range

    Watch.instance.start(Watch.PerfSpan.Bnext);
    try {
      remoteB.seek(rBT, Collections.<ByteSequence>emptySet(), false);
    } finally {
      Watch.instance.stop(Watch.PerfSpan.Bnext);
    }

    prepNextRowMatch(false);
  }

  private void prepNextRowMatch(boolean doNext) throws IOException {
    if (!remoteAT.hasTop() || !remoteB.hasTop()) {
      bottomIter = null;
      return;
    }
    if (doNext) {
      Watch.instance.start(Watch.PerfSpan.ATnext);
      try {
        remoteAT.next();
      } finally {
        Watch.instance.stop(Watch.PerfSpan.ATnext);
      }
      if (!remoteAT.hasTop()) {
        bottomIter = null;
        return;
      }
      Watch.instance.start(Watch.PerfSpan.Bnext);
      try {
        remoteB.next();
      } finally {
        Watch.instance.stop(Watch.PerfSpan.Bnext);
      }
      if (!remoteB.hasTop()) {
        bottomIter = null;
        return;
      }
    }

    SortedMap<Key, Value> ArowMap, BrowMap;
    Text Arow = remoteAT.getTopKey().getRow(), Brow = remoteB.getTopKey().getRow();
    int cmp = Arow.compareTo(Brow);
    while (cmp != 0) {
      if (cmp < 0) {
        skipNextRowUntil(remoteAT, Brow, Watch.PerfSpan.ATnext);
        if (!remoteAT.hasTop()) {
          bottomIter = null;
          return;
        }
        remoteAT.getTopKey().getRow(Arow);
      } else if (cmp > 0) {
        skipNextRowUntil(remoteB, Arow, Watch.PerfSpan.Bnext);
        if (!remoteB.hasTop()) {
          bottomIter = null;
          return;
        }
        remoteB.getTopKey().getRow(Brow);
      }
      cmp = Arow.compareTo(Brow);
    }
    //assert cmp == 0;
    Watch.instance.start(Watch.PerfSpan.RowDecodeBoth);
    try {
      ArowMap = readRow(remoteAT, Watch.PerfSpan.ATnext);
      BrowMap = readRow(remoteB, Watch.PerfSpan.Bnext);
    } finally {
      Watch.instance.stop(Watch.PerfSpan.RowDecodeBoth);
    }

    bottomIter = new PeekingIterator<>(new CartesianDotIter(ArowMap, BrowMap, multiplyOp));
    assert hasTop();
  }

  /**
   * Call next() on skvi until getTopKey() is a new row, or until !hasTop().
   * Todo P2: Experiment with using seek() if this takes a while, say greater than 10 next() calls.
   *
   * @return True if advanced to a new row; false if !hasTop().
   */
  static boolean skipNextRow(SortedKeyValueIterator<Key, Value> skvi) throws IOException {
    if (!skvi.hasTop())
      throw new IllegalStateException(skvi + " should hasTop()");
    Text curRow = skvi.getTopKey().getRow();
    Text newRow = new Text(curRow);
    do {
      skvi.next();
    } while (skvi.hasTop() && curRow.equals(skvi.getTopKey().getRow(newRow)));
    return skvi.hasTop();
  }

  /**
   * Call next() on skvi until getTopKey() advances to a row >= rowToSkipUntil, or until !hasTop().
   * Todo P2: Experiment with using seek() if this takes a while, say greater than 10 next() calls.
   * Todo P2: Replace with getRowData() for efficiency (less copying).
   *
   * @return True if advanced to a new row; false if !hasTop().
   */
  static boolean skipNextRowUntil(SortedKeyValueIterator<Key, Value> skvi, Text rowToSkipUntil, Watch.PerfSpan watch) throws IOException {
    Text curRow = skvi.getTopKey().getRow();
    long cnt = 0;
    while (skvi.hasTop() && rowToSkipUntil.compareTo(skvi.getTopKey().getRow(curRow)) > 0) {
      cnt++;
      Watch.instance.start(watch);
      try {
        skvi.next();
      } finally {
        Watch.instance.stop(watch);
      }
    }
    Watch.instance.increment(Watch.PerfSpan.RowSkipNum, cnt);
    return skvi.hasTop();
  }

  /**
   * Fill a SortedMap with all the entries in the same row as skvi.getTopKey().getRow()
   * when first called.
   * Postcondition: !skvi.hasTop() || skvi.getTopKey().getRow() has changed.
   *
   * @return Sorted map of the entries.
   * Todo P2: replace SortedMap with a list of entries, since sorted order is guaranteed
   */
  static SortedMap<Key, Value> readRow(SortedKeyValueIterator<Key, Value> skvi, Watch.PerfSpan watch) throws IOException {
    if (!skvi.hasTop())
      throw new IllegalStateException(skvi + " should hasTop()");
    Text thisRow = skvi.getTopKey().getRow();
    Text curRow = new Text(thisRow);
    SortedMap<Key, Value> map = new TreeMap<>();
    do {
      map.put(skvi.getTopKey(), skvi.getTopValue());
      Watch.instance.start(watch);
      try {
        skvi.next();
      } finally {
        Watch.instance.stop(watch);
      }
    } while (skvi.hasTop() && skvi.getTopKey().getRow(curRow).equals(thisRow));
    return map;
  }

  static class CartesianDotIter implements Iterator<Map.Entry<Key, Value>> {
    private SortedMap<Key, Value> ArowMap, BrowMap;
    private PeekingIterator<Map.Entry<Key, Value>> ArowMapIter;
    private Iterator<Map.Entry<Key, Value>> BrowMapIter;
    private IMultiplyOp multiplyOp;

    public CartesianDotIter(SortedMap<Key, Value> arowMap, SortedMap<Key, Value> browMap, IMultiplyOp multiplyOp) {
      ArowMap = arowMap;
      BrowMap = browMap;
      ArowMapIter = new PeekingIterator<>(ArowMap.entrySet().iterator());
      BrowMapIter = BrowMap.entrySet().iterator();
      this.multiplyOp = multiplyOp;
    }

    @Override
    public boolean hasNext() {
      return BrowMapIter.hasNext();
    }

    @Override
    public Map.Entry<Key, Value> next() {
      Map.Entry<Key, Value> eA, eB = BrowMapIter.next();
      if (!BrowMapIter.hasNext()) {
        eA = ArowMapIter.next(); // advance ArowMapIter
        if (ArowMapIter.hasNext())
          BrowMapIter = BrowMap.entrySet().iterator(); // STOP if no more ArowMapIter
      } else
        eA = ArowMapIter.peek();
      return multiplyEntry(eA, eB);
    }

    Map.Entry<Key, Value> multiplyEntry(Map.Entry<Key, Value> e1, Map.Entry<Key, Value> e2) {
      assert e1.getKey().getRowData().compareTo(e2.getKey().getRowData()) == 0;
      Key k1 = e1.getKey(), k2 = e2.getKey();
      final Key k = new Key(k1.getColumnQualifier(), k1.getColumnFamily(), k2.getColumnQualifier(), System.currentTimeMillis());
      final Value v = multiplyOp.multiply(e1.getValue(), e2.getValue());
      return new Map.Entry<Key, Value>() {
        @Override
        public Key getKey() {
          return k;
        }

        @Override
        public Value getValue() {
          return v;
        }

        @Override
        public Value setValue(Value value) {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

//  static class DotIter implements Iterator<Map.Entry<Key, Value>> {
//    private Iterator<Map.Entry<Key, Value>> i1, i2;
//    private static Comparator<Key> comparator = new ColFamilyQualifierComparator();
//    private IMultiplyOp multiplyOp;
//    private Map.Entry<Key, Value> nextEntry;
//
//    Map.Entry<Key, Value> multiplyEntry(Map.Entry<Key, Value> e1, Map.Entry<Key, Value> e2) {
//      assert comparator.compare(e1.getKey(), e2.getKey()) == 0;
//      Key k1 = e1.getKey(), k2 = e2.getKey();
//      final Key k = new Key(k1.getRow(), k1.getColumnFamily(), k2.getRow(), System.currentTimeMillis());
//      final Value v = multiplyOp.multiply(e1.getValue(), e2.getValue());
//      return new Map.Entry<Key, Value>() {
//        @Override
//        public Key getKey() {
//          return k;
//        }
//
//        @Override
//        public Value getValue() {
//          return v;
//        }
//
//        @Override
//        public Value setValue(Value value) {
//          throw new UnsupportedOperationException();
//        }
//      };
//    }
//
//    public DotIter(Iterator<Map.Entry<Key, Value>> iter1,
//                   Iterator<Map.Entry<Key, Value>> iter2,
//                   IMultiplyOp multiplyOp) {
//      this.i1 = iter1;
//      this.i2 = iter2;
//      this.multiplyOp = multiplyOp;
//      //            this.comparator = comparator;
//      nextEntry = prepareNext();
//    }
//
//    @Override
//    public boolean hasNext() {
//      return nextEntry != null;
//    }
//
//    @Override
//    public Map.Entry<Key, Value> next() {
//      Map.Entry<Key, Value> ret = nextEntry;
//      nextEntry = prepareNext();
//      return ret;
//    }
//
//    private Map.Entry<Key, Value> prepareNext() {
//      if (!i1.hasNext() || !i2.hasNext()) {
//        return null;
//      }
//      Map.Entry<Key, Value> e1 = i1.next();
//      Map.Entry<Key, Value> e2 = i2.next();
//      int cmp = comparator.compare(e1.getKey(), e2.getKey());
//      while (cmp < 0 && i1.hasNext() || cmp > 0 && i2.hasNext()) {
//        if (cmp < 0) {
//          e1 = i1.next();
//        } else if (cmp > 0) {
//          e2 = i2.next();
//        }
//        cmp = comparator.compare(e1.getKey(), e2.getKey());
//      }
//      return cmp == 0 ? multiplyEntry(e1, e2) : null;
//    }
//
//    @Override
//    public void remove() {
//      throw new UnsupportedOperationException();
//    }
//  }


  @Override
  public void next() throws IOException {
    bottomIter.next();
    if (!bottomIter.hasNext())
      prepNextRowMatch(false);
  }

  /**
   * Compare only the column family and column qualifier.
   */
  static class ColFamilyQualifierComparator implements Comparator<Key> {
    private Text text = new Text();

    @Override
    public int compare(Key k1, Key k2) {
      k2.getColumnFamily(text);
      int cfam = k1.compareColumnFamily(text);
      if (cfam != 0)
        return cfam;
      k2.getColumnQualifier(text);
      return k1.compareColumnQualifier(text);
    }
  }


  @Override
  public boolean hasTop() {
    assert bottomIter == null || bottomIter.hasNext();
    return bottomIter != null;
  }

  @Override
  public Key getTopKey() {
    return bottomIter.peek().getKey();
  }

  @Override
  public Value getTopValue() {
    return bottomIter.peek().getValue();
  }

  @Override
  public DotIterator deepCopy(IteratorEnvironment env) {
    DotIterator copy = new DotIterator();
    copy.remoteAT = remoteAT.deepCopy(env);
    copy.remoteB = remoteB.deepCopy(env);
    return copy;
  }
}
