package edu.mit.ll.graphulo;

import com.google.common.collect.Iterators;
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
 * Performs operations on two tables.
 * When <tt>dot == ROW_CARTESIAN</tt>, acts as multiply step of outer product, emitting partial products.
 * When <tt>dot == ROW_COLF_COLQ_MATCH</tt>, acts as element-wise multiply.
 * When <tt>dot == NONE</tt>, no multiplication.
 * Set <tt>AT.emitNoMatch = B.emitNoMatch = true</tt> for sum of tables.
 * <p/>
 * Configure two remote sources for tables AT and B,
 * or configure one remote source and use the parent source as the other one.
 * <p/>
 * Table of behavior given options for AT, options for B and a parent source iterator.
 * <table>
 * <tr><th>AT</th><th>B</th><th>source</th><th>Behavior</th></tr>
 * <tr><td>y</td><td>y</td><td>y</td><td>Warn. AT * B</td></tr>
 * <tr><td>y</td><td>y</td><td>n</td><td>AT * B</td></tr>
 * <tr><td>y</td><td>n</td><td>n</td><td>AT * AT</td></tr>
 * <tr><td>y</td><td>n</td><td>y</td><td>AT * source</td></tr>
 * <tr><td>n</td><td>y</td><td>y</td><td>source * B</td></tr>
 * <tr><td>n</td><td>y</td><td>n</td><td>B * B</td></tr>
 * <tr><td>n</td><td>n</td><td>y</td><td>source * source</td></tr>
 * <tr><td>n</td><td>n</td><td>n</td><td>Error.</td></tr>
 * </table>
 */
public class DotIterator implements SaveStateIterator, OptionDescriber {
  private static final Logger log = LogManager.getLogger(DotIterator.class);

  private IMultiplyOp multiplyOp = new BigDecimalMultiply();
  private SortedKeyValueIterator<Key, Value> remoteAT, remoteB;
  private boolean emitNoMatchA = false, emitNoMatchB = false;
  private PeekingIterator2<Map.Entry<Key, Value>> bottomIter; // = new TreeMap<>(new ColFamilyQualifierComparator());
  private Range seekRange;
  private DOT_TYPE dot = DOT_TYPE.NONE;
  private Collection<ByteSequence> seekColumnFamilies;
  private boolean seekInclusive;

  public static final String PREFIX_AT = "AT";
  public static final String PREFIX_B = "B";

  public enum DOT_TYPE {
    NONE, ROW_CARTESIAN, ROW_COLF_COLQ_MATCH
  }

  static final OptionDescriber.IteratorOptions iteratorOptions;

  static {
    final Map<String, String> optDesc = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
      optDesc.put(PREFIX_AT + '.'+entry.getKey(), "Table AT:" + entry.getValue());
    }
    for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
      optDesc.put(PREFIX_B + '.'+entry.getKey(), "Table B:" + entry.getValue());
    }

    optDesc.put("dot", "Type of dot product: NONE, ROW_CARTESIAN, ROW_COLF_COLQ_MATCH");
    optDesc.put(PREFIX_AT+'.'+"emitNoMatch", "Emit entries that do not match the other table");
    optDesc.put(PREFIX_B+'.'+"emitNoMatch", "Emit entries that do not match the other table");

    iteratorOptions = new OptionDescriber.IteratorOptions("DotMultIterator",
        "Outer product on A and B, given AT and B. Does not sum.",
        optDesc, null);
  }


  public DotIterator() {
  }

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
      if (key.equals(PREFIX_AT+'.'+"emitNoMatch") || key.equals(PREFIX_B+'.'+"emitNoMatch"))
        //noinspection ResultOfMethodCallIgnored
        Boolean.parseBoolean(entry.getValue());
      else if (key.startsWith(PREFIX_AT))
        optAT.put(key.substring(PREFIX_AT.length()), entry.getValue());
      else if (key.startsWith(PREFIX_B))
        optB.put(key.substring(PREFIX_B.length()), entry.getValue());
      else switch (key) {
          case "dot":
            DOT_TYPE.valueOf(entry.getValue());
          case "multiplyOp":
            Class<?> c;
            try {
              c = Class.forName(entry.getValue());
            } catch (ClassNotFoundException e) {
              throw new IllegalArgumentException("Can't find multiplyOp class: " + entry.getValue(), e);
            }
            try {
              c.asSubclass(IMultiplyOp.class);
            } catch (ClassCastException e) {
              throw new IllegalArgumentException("multiplyOp is not a subclass of IMultiplyOp: " + c.getName(), e);
            }
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
            if (v != null && Boolean.parseBoolean(v))
              log.warn("Forcing doWholeRow option on table A to FALSE. Given: " + v);
            v = entryMap.remove("emitNoMatch");
            if (v != null)
              emitNoMatchA = Boolean.parseBoolean(v);
            optAT = entryMap;
            optAT.put("doWholeRow", "false");
            break;
          }
          case PREFIX_B: {
            String v = entryMap.remove("doWholeRow");
            if (v != null && Boolean.parseBoolean(v)) {
              log.warn("Forcing doWholeRow option on table BT to FALSE. Given: " + v);
            }
            v = entryMap.remove("emitNoMatch");
            if (v != null)
              emitNoMatchB = Boolean.parseBoolean(v);
            optB = entryMap;
            optB.put("doWholeRow", "false");
            break;
          }
          case "": {
            for (Map.Entry<String, String> entry : entryMap.entrySet()) {
              switch (entry.getKey()) {
                case "dot":
                  dot = DOT_TYPE.valueOf(entry.getValue());
                  break;
                case "multiplyOp":
                  Class<?> c;
                  try {
                    c = Class.forName(entry.getValue());
                  } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException("Can't find multiplyOp class: " + entry.getValue(), e);
                  }
                  Class<? extends IMultiplyOp> cm;
                  try {
                    cm = c.asSubclass(IMultiplyOp.class);
                  } catch (ClassCastException e) {
                    throw new IllegalArgumentException("multiplyOp is not a subclass of IMultiplyOp: " + c.getName(), e);
                  }
                  try {
                    multiplyOp = cm.newInstance();
                  } catch (InstantiationException | IllegalAccessException e) {
//                    log.warn("can't instantiate new instance of "+cm.getName(), e);
                    throw new IllegalArgumentException("can't instantiate new instance of " + cm.getName(), e);
                  }
                  break;
                default:
                  log.warn("Unrecognized option: " + prefix + '.' + entry);
                  break;
              }
            }
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
      } else {
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

  private static Map.Entry<Key,Value> copyTopEntry(SortedKeyValueIterator<Key,Value> skvi) {
    final Key k = GraphuloUtil.keyCopy(skvi.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL);
    final Value v = new Value(skvi.getTopValue());
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


  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
//    System.out.println("DM ori range: "+range);
//    System.out.println("DM colFamili: "+columnFamilies);
//    System.out.println("DM inclusive: " + inclusive);

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

    seekRange = range;
    seekColumnFamilies = columnFamilies;
    seekInclusive = inclusive;
    System.out.println("DM adj range: " + range);

    // Weird results if we start in the middle of a row. Not handling.
    Watch<Watch.PerfSpan> watch = Watch.getInstance();
    watch.start(Watch.PerfSpan.ATnext);
    try {
      remoteAT.seek(seekRange, columnFamilies, inclusive);
    } finally {
      watch.stop(Watch.PerfSpan.ATnext);
    }

    watch.start(Watch.PerfSpan.Bnext);
    try {
      remoteB.seek(seekRange, columnFamilies, inclusive);
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

    if (dot == DOT_TYPE.ROW_CARTESIAN || dot == DOT_TYPE.ROW_COLF_COLQ_MATCH) {
      PartialKey pk = null;
      if (dot == DOT_TYPE.ROW_CARTESIAN)
        pk = PartialKey.ROW;
      else if (dot == DOT_TYPE.ROW_COLF_COLQ_MATCH)
        pk = PartialKey.ROW_COLFAM_COLQUAL;


      Watch<Watch.PerfSpan> watch = Watch.getInstance();
      do {
        int cmp = remoteAT.getTopKey().compareTo(remoteB.getTopKey(), pk);
        while (cmp != 0) {
          if (cmp < 0) {
            if (emitNoMatchA) {
              bottomIter = new PeekingIterator2<>(Iterators.singletonIterator(
                      copyTopEntry(remoteAT)));
              remoteAT.next();
              return;
            }
            boolean success = skipUntil(remoteAT, remoteB.getTopKey(), pk, seekRange, seekColumnFamilies, seekInclusive, watch, Watch.PerfSpan.ATnext);
            if (!success) {
              bottomIter = null;
              return;
            }
          } else if (cmp > 0) {
            if (emitNoMatchB) {
              bottomIter = new PeekingIterator2<>(Iterators.singletonIterator(
                  copyTopEntry(remoteB)));
              remoteB.next();
              return;
            }
            boolean success = skipUntil(remoteB, remoteAT.getTopKey(), pk, seekRange, seekColumnFamilies, seekInclusive, watch, Watch.PerfSpan.Bnext);
            if (!success) {
              bottomIter = null;
              return;
            }
          }
          cmp = remoteAT.getTopKey().compareTo(remoteB.getTopKey(), pk);
        }
        //assert cmp == 0;

        if (dot == DOT_TYPE.ROW_CARTESIAN) {
          SortedMap<Key, Value> ArowMap, BrowMap;
          watch.start(Watch.PerfSpan.RowDecodeBoth);
          try {
            ArowMap = readRow(remoteAT, watch, Watch.PerfSpan.ATnext);
            BrowMap = readRow(remoteB, watch, Watch.PerfSpan.Bnext);
          } finally {
            watch.stop(Watch.PerfSpan.RowDecodeBoth);
          }
          bottomIter = new PeekingIterator2<>(new CartesianDotIter(ArowMap, BrowMap, multiplyOp));
        } else if (dot == DOT_TYPE.ROW_COLF_COLQ_MATCH) {
          bottomIter = new PeekingIterator2<>(
              Iterators.singletonIterator(
                  multiplyOp.multiplyEntry(remoteAT.getTopKey().getRowData(), remoteAT.getTopKey().getColumnFamilyData(),
                      remoteAT.getTopKey().getColumnQualifierData(), remoteB.getTopKey().getColumnFamilyData(),
                      remoteB.getTopKey().getColumnQualifierData(), remoteAT.getTopValue(), remoteB.getTopValue())
              ));
          remoteAT.next();
          remoteB.next();
        }
      } while (bottomIter != null && !bottomIter.hasNext());
      assert hasTop();

    }
  }

  /**
   * Call next() on skvi until getTopKey() advances >= keyToSkipTo (in terms of pk), or until !hasTop().
   * Calls seek() if this takes a while, say greater than 10 next() calls.
   *
   * @return True if advanced to a new key; false if !hasTop().
   */
  static boolean skipUntil(SortedKeyValueIterator<Key, Value> skvi, Key keyToSkipTo, PartialKey pk,
                           Range seekRange, Collection<ByteSequence> columnFamilies, boolean inclusive,
                           Watch<Watch.PerfSpan> watch, Watch.PerfSpan watchtype) throws IOException {
    assert keyToSkipTo != null;
    /** Call seek() if using this many next() calls does not get us to rowToSkipTo */
    final int MAX_NEXT_ATTEMPT = 10;
    int cnt;
    for (cnt = 0;
         cnt < MAX_NEXT_ATTEMPT && skvi.hasTop() && keyToSkipTo.compareTo(skvi.getTopKey(), pk) > 0;
         cnt++) {
      watch.start(watchtype);
      try {
        skvi.next();
      } finally {
        watch.stop(watchtype);
      }
    }
    if (skvi.hasTop() && keyToSkipTo.compareTo(skvi.getTopKey(), pk) > 0) {
      // set target range to beginning of pk
      Key seekKey = GraphuloUtil.keyCopy(keyToSkipTo, pk);
      Range skipToRange = new Range(seekKey, true, null, false)
          .clip(seekRange, true);
      if (skipToRange == null) // row we want to get to does not exist, and it is out of our range
        return false;
      skvi.seek(skipToRange, columnFamilies, inclusive);
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

  /**
   * Emits Cartesian product of provided iterators, passed to multiply function.
   */
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
    if (bottomIter == null || bottomIter.peekSecond() != null) {
      // either we have no entries left to emit, or we need to
      // finish the row's cartesian product first (until bottomIter has one left)
      return null;
    } else {
      // the current top entry of bottomIter is the last in this cartesian product (bottomIter)
      // Save state at this row.  If reseek'd to this row, go to the next row (assume exclusive).
      assert bottomIter.peekFirst() != null;
      final Key k =
          dot == DOT_TYPE.ROW_CARTESIAN
            ? GraphuloUtil.keyCopy(bottomIter.peekFirst().getKey(), PartialKey.ROW)
            : bottomIter.peekFirst().getKey(); // second case should be okay without copying
      final Value v = new Value(); // no additional information to return.
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
