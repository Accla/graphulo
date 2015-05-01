package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.mult.BigDecimalMultiply;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Multiply step of outer product, emitting partial products.
 * Configure two remote sources for tables AT and B,
 * or configure one remote source and use the parent source as the other one.
 * <p/>
 * Table of behavior given options for AT, options for B and a parent source iterator.
 * <table>
 *   <tr><th>AT</th><th>B</th><th>source</th><th>Behavior</th></tr>
 *   <tr><td>y</td><td>y</td><td>y</td><td>Warn. AT * B</td></tr>
 *   <tr><td>y</td><td>y</td><td>n</td><td>AT * B</td></tr>
 *   <tr><td>y</td><td>n</td><td>n</td><td>AT * AT</td></tr>
 *   <tr><td>y</td><td>n</td><td>y</td><td>AT * source</td></tr>
 *   <tr><td>n</td><td>y</td><td>y</td><td>source * B</td></tr>
 *   <tr><td>n</td><td>y</td><td>n</td><td>B * B</td></tr>
 *   <tr><td>n</td><td>n</td><td>y</td><td>source * source</td></tr>
 *   <tr><td>n</td><td>n</td><td>n</td><td>Error.</td></tr>
 * </table>
 */
public class DotIterator implements SaveStateIterator, OptionDescriber {
  private static final Logger log = LogManager.getLogger(DotIterator.class);

  private IMultiplyOp multiplyOp = new BigDecimalMultiply();
  private SortedKeyValueIterator<Key,Value> remoteAT, remoteB;

  private PeekingIterator2<Map.Entry<Key, Value>> bottomIter; // = new TreeMap<>(new ColFamilyQualifierComparator());

  public static final String PREFIX_AT = "AT";
  public static final String PREFIX_B = "B";

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
        "Outer product on A and B, given AT and B. Does not sum.",
        optDesc, null);
  }

  private Text curRowMatch;
  private Range seekRange;

  public DotIterator() {}

  DotIterator(DotIterator other) {
    this.multiplyOp = other.multiplyOp;
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
      if (entry.getValue().isEmpty())
        continue;
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
    return
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
            if (v != null && Boolean.parseBoolean(v)) {
              log.warn("Forcing doWholeRow option on table A to FALSE. Given: " + v);
            }
            optAT = entryMap;
            optAT.put("doWholeRow", "false");
            break;
          }
          case PREFIX_B: {
            String v = entryMap.remove("doWholeRow");
            if (v != null && Boolean.parseBoolean(v)) {
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

    if (optAT == null && optB == null && source == null) { // ~A ~B ~S
      throw new IllegalArgumentException("optAT, optB, and source cannot all be null");
    }
    if (optAT != null && optB != null && source != null) { // A B S
      log.warn("DotMultIterator ignores/replaces parent source passed in init(): " + source);
    }
    if (optAT == null && optB == null) {      // ~A ~B S
      remoteAT = source;
      remoteB = source.deepCopy(env);
    } else if (optAT != null) {
      remoteAT = new RemoteSourceIterator();
      remoteAT.init(null, optAT, env);
      if (optB == null) {
        if (source == null)
          remoteB = remoteAT.deepCopy(env);   // A ~B ~S
        else
          remoteB = source;                   // A ~B S
      }
      else {
        remoteB = new RemoteSourceIterator(); // A B _
        remoteB.init(null, optB, env);
      }
    } else {
      remoteB = new RemoteSourceIterator();
      remoteB.init(null, optB, env);
      if (source != null)                     // ~A B S
        remoteAT = source;
      else
        remoteAT = remoteB.deepCopy(env);     // ~A B ~S
    }
  }


  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    Range rAT, rB;
    System.out.println("DM ori range: "+range);
    System.out.println("DM colFamili: "+columnFamilies);
    System.out.println("DM inclusive: " + inclusive);

    Key sk = range.getStartKey();
    // put range at beginning of row, no matter what
    if (sk != null)
      range = new Range(new Key(sk.getRow()), true, range.getEndKey(), range.isEndKeyInclusive());

    // if range is not infinite, see if there is a clear sign we want to restore state:
    if (sk != null && sk.getColumnFamilyData().length() == 0 && sk.getColumnQualifierData().length() == 0 && sk.getColumnVisibilityData().length() == 0
        && sk.getTimestamp() == Long.MAX_VALUE && !range.isStartKeyInclusive()) {
      // assuming that we are seeking using a key previously returned by this iterator
      // therefore go to the next row
      Key followingRowKey = sk.followingKey(PartialKey.ROW);
      if (range.getEndKey() != null && followingRowKey.compareTo(range.getEndKey()) > 0)
        return;

      range = new Range(followingRowKey, true, range.getEndKey(), range.isEndKeyInclusive());
    }

    rAT = rB = seekRange = range;
    System.out.println("DM adj range: "+range);
//    log.debug("seek range: " + range);
    log.debug("rAT/B rnge: " + rAT);
//    log.debug("rB  range: " + rB);

    // Weird results if we start in the middle of a row. Not handling.
    Watch<Watch.PerfSpan> watch = Watch.getInstance();
    watch.start(Watch.PerfSpan.ATnext);
    try {
      remoteAT.seek(rAT, Collections.<ByteSequence>emptySet(), false);
    } finally {
      watch.stop(Watch.PerfSpan.ATnext);
    }

    watch.start(Watch.PerfSpan.Bnext);
    try {
      remoteB.seek(rB, Collections.<ByteSequence>emptySet(), false);
    } finally {
      watch.stop(Watch.PerfSpan.Bnext);
    }

    prepNextRowMatch(/*false*/);
  }

  private void prepNextRowMatch(/*boolean doNext*/) throws IOException {
    if (!remoteAT.hasTop() || !remoteB.hasTop()) {
      bottomIter = null;
      return;
    }
    /*if (doNext) {
      watch.start(Watch.PerfSpan.ATnext);
      try {
        remoteAT.next();
      } finally {
        watch.stop(Watch.PerfSpan.ATnext);
      }
      if (!remoteAT.hasTop()) {
        bottomIter = null;
        return;
      }
      watch.start(Watch.PerfSpan.Bnext);
      try {
        remoteB.next();
      } finally {
        watch.stop(Watch.PerfSpan.Bnext);
      }
      if (!remoteB.hasTop()) {
        bottomIter = null;
        return;
      }
    }*/
    Watch<Watch.PerfSpan> watch = Watch.getInstance();
    Text Arow = remoteAT.getTopKey().getRow(), Brow = remoteB.getTopKey().getRow();
    int cmp = Arow.compareTo(Brow);
    while (cmp != 0) {
      if (cmp < 0) {
        boolean success = skipNextRowUntil(remoteAT, Brow, seekRange, watch, Watch.PerfSpan.ATnext);
        if (!success) {
          bottomIter = null;
          return;
        }
        remoteAT.getTopKey().getRow(Arow);
      } else if (cmp > 0) {
        boolean success = skipNextRowUntil(remoteB, Arow, seekRange, watch, Watch.PerfSpan.Bnext);
        if (!success) {
          bottomIter = null;
          return;
        }
        remoteB.getTopKey().getRow(Brow);
      }
      cmp = Arow.compareTo(Brow);
    }
    //assert cmp == 0;
    SortedMap<Key, Value> ArowMap, BrowMap;

    watch.start(Watch.PerfSpan.RowDecodeBoth);
    try {
      ArowMap = readRow(remoteAT, watch, Watch.PerfSpan.ATnext);
      BrowMap = readRow(remoteB, watch, Watch.PerfSpan.Bnext);
    } finally {
      watch.stop(Watch.PerfSpan.RowDecodeBoth);
    }

    curRowMatch = Arow;
    bottomIter = new PeekingIterator2<>(new CartesianDotIter(ArowMap, BrowMap, multiplyOp));
    assert hasTop();
  }

//  /**
//   * Call next() on skvi until getTopKey() is a new row, or until !hasTop().
//   * Todo P2: Experiment with using seek() if this takes a while, say greater than 10 next() calls.
//   *
//   * @return True if advanced to a new row; false if !hasTop().
//   */
//  static boolean skipNextRow(SortedKeyValueIterator<Key, Value> skvi) throws IOException {
//    if (!skvi.hasTop())
//      throw new IllegalStateException(skvi + " should hasTop()");
//    Text curRow = skvi.getTopKey().getRow();
//    Text newRow = new Text(curRow);
//    do {
//      skvi.next();
//    } while (skvi.hasTop() && curRow.equals(skvi.getTopKey().getRow(newRow)));
//    return skvi.hasTop();
//  }

  /**
   * Call next() on skvi until getTopKey() advances to a row >= rowToSkipTo, or until !hasTop().
   * Calls seek() if this takes a while, say greater than 10 next() calls.
   * Todo P2: Replace with getRowData() for efficiency (less copying).
   *
   * @return True if advanced to a new row; false if !hasTop().
   */
  static boolean skipNextRowUntil(SortedKeyValueIterator<Key, Value> skvi, Text rowToSkipTo,
                                  Range seekRange, Watch<Watch.PerfSpan> watch, Watch.PerfSpan watchtype) throws IOException {
    assert rowToSkipTo != null;
    /** Call seek() if using this many next() calls does not get us to rowToSkipTo */
    final int MAX_NEXT_ATTEMPT = 10;

    Text curRow = skvi.getTopKey().getRow();
    int cnt = 0;
    while (cnt < MAX_NEXT_ATTEMPT && skvi.hasTop() && rowToSkipTo.compareTo(skvi.getTopKey().getRow(curRow)) > 0) {
      cnt++;
      watch.start(watchtype);
      try {
        skvi.next();
      } finally {
        watch.stop(watchtype);
      }
    }
    // seek if we didn't get past the desired row in 10 next() calls
    if (skvi.hasTop() && rowToSkipTo.compareTo(skvi.getTopKey().getRow(curRow)) > 0) {
      Range skipToRange = new Range(rowToSkipTo, true, null, false);
      skipToRange = skipToRange.clip(seekRange, true);
      if (skipToRange == null) // row we want to get to does not exist, and it is out of our range
        return false;
      skvi.seek(skipToRange, Collections.<ByteSequence>emptySet(), false);
    }

    watch.increment(Watch.PerfSpan.RowSkipNum, cnt);
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
  static SortedMap<Key, Value> readRow(SortedKeyValueIterator<Key, Value> skvi, Watch<Watch.PerfSpan> watch, Watch.PerfSpan watchtype) throws IOException {
    if (!skvi.hasTop())
      throw new IllegalStateException(skvi + " should hasTop()");
    Text thisRow = skvi.getTopKey().getRow();
    Text curRow = new Text(thisRow);
    SortedMap<Key, Value> map = new TreeMap<>();
    do {
      map.put(skvi.getTopKey(), new Value(skvi.getTopValue()));
      watch.start(watchtype);
      try {
        skvi.next();
      } finally {
        watch.stop(watchtype);
      }
    } while (skvi.hasTop() && skvi.getTopKey().getRow(curRow).equals(thisRow));
    return map;
  }

  /** Emits Cartesian product of provided iterators, passed to multiply function. */
  static class CartesianDotIter implements Iterator<Map.Entry<Key, Value>> {
    private SortedMap<Key, Value> ArowMap, BrowMap;
    private PeekingIterator1<Map.Entry<Key, Value>> ArowMapIter;
    private Iterator<Map.Entry<Key, Value>> BrowMapIter;
    private IMultiplyOp multiplyOp;

    public CartesianDotIter(SortedMap<Key, Value> arowMap, SortedMap<Key, Value> browMap, IMultiplyOp multiplyOp) {
      ArowMap = arowMap;
      BrowMap = browMap;
      ArowMapIter = new PeekingIterator1<>(ArowMap.entrySet().iterator());
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

    private Map.Entry<Key, Value> multiplyEntry(Map.Entry<Key, Value> e1, Map.Entry<Key, Value> e2) {
      assert e1.getKey().getRowData().compareTo(e2.getKey().getRowData()) == 0;
      Key k1 = e1.getKey(), k2 = e2.getKey();
      return multiplyOp.multiplyEntry(k1.getRowData(), k1.getColumnFamilyData(), k1.getColumnQualifierData(),
          k2.getColumnFamilyData(), k2.getColumnQualifierData(), e1.getValue(), e2.getValue());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }




  @Override
  public void next() throws IOException {
    bottomIter.next();
    if (!bottomIter.hasNext())
      prepNextRowMatch(/*false*/);
  }

  @Override
  public Map.Entry<Key, Value> safeState() {
    if (bottomIter != null && bottomIter.peekSecond() != null) {
      // finish the row's cartesian product first
      return null;
    } else {
      // the current top entry of bottomIter is the last in this cartesian product.
      // Save state at this row.  If reseek'd to this row, go to the next row (assume exclusive).
      final Key k = new Key(this.curRowMatch);
      final Value v = new Value(); // no additional information to return.
      return new Map.Entry<Key,Value>() {
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
  }


  @Override
  public boolean hasTop() {
    assert bottomIter == null || bottomIter.hasNext();
    return bottomIter != null;
  }

  @Override
  public Key getTopKey() {
    return bottomIter.peekFirst().getKey();
  }

  @Override
  public Value getTopValue() {
    return bottomIter.peekFirst().getValue();
  }

  @Override
  public DotIterator deepCopy(IteratorEnvironment env) {
    DotIterator copy = new DotIterator(this);
    copy.remoteAT = remoteAT.deepCopy(env);
    copy.remoteB = remoteB.deepCopy(env);
    return copy;
  }
}
