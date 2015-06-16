package edu.mit.ll.graphulo.skvi;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.mult.BigDecimalMultiply;
import edu.mit.ll.graphulo.mult.IMultiplyOp;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.PeekingIterator1;
import edu.mit.ll.graphulo.util.PeekingIterator2;
import edu.mit.ll.graphulo.util.SKVIRowIterator;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Performs operations on two tables.
 * When <tt>dot == TWOROW</tt>, acts as multiply step of outer product, emitting partial products.
 * When <tt>dot == EWISE</tt>, acts as element-wise multiply.
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
 *
 * If either the AT or B tableName is "{@value #CLONESOURCE_TABLENAME}",
 * then TwoTableIterator will initalize that table as a deepCopy of the source skvi,
 * setting up rowRanges and colFilters as in the options.
 *
 */
public class TwoTableIterator implements SaveStateIterator, OptionDescriber {
  private static final Logger log = LogManager.getLogger(TwoTableIterator.class);

  private IMultiplyOp multiplyOp = new BigDecimalMultiply();
  private Map<String, String> multiplyOpOptions = new HashMap<>();
  private SortedKeyValueIterator<Key, Value> remoteAT, remoteB;
  private boolean emitNoMatchA = false, emitNoMatchB = false;
  private PeekingIterator2<? extends Map.Entry<Key, Value>> bottomIter; // = new TreeMap<>(new ColFamilyQualifierComparator());
  private Range seekRange;
  private DOT_TYPE dot = DOT_TYPE.NONE;
  private Collection<ByteSequence> seekColumnFamilies;
  private boolean seekInclusive;
  /**
   * Track the row of AT and B emitted. For monitoring.
   */
  private Key emitted = new Key();

  public static final String CLONESOURCE_TABLENAME = "*CLONESOURCE*";
  public static final String PREFIX_AT = "AT";
  public static final String PREFIX_B = "B";

  public enum DOT_TYPE {
    /** Not yet implemented.  Plan is to emit nothing from collisions.
     * Only emit entries that do not match from the two tables. Set emitNoMatch to true for the two tables. */
    NONE,
    /** Read both rows into memory. */
    TWOROW,
    /** Read a row from A into memory and stream/iterate through columns in the row from B. */
    ONEROWA,
    /** Read a row from B into memory and stream/iterate through columns in the row from A. */
    ONEROWB,
    /** Match on row and column. Nothing extra read into memory. */
    EWISE
  }

  static final OptionDescriber.IteratorOptions iteratorOptions;

  static {
    final Map<String, String> optDesc = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
      optDesc.put(PREFIX_AT + '.' + entry.getKey(), "Table AT:" + entry.getValue());
    }
    for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
      optDesc.put(PREFIX_B + '.' + entry.getKey(), "Table B:" + entry.getValue());
    }

    optDesc.put("dot", "Type of dot product: NONE, TWOROW, ONEROWA, ONEROWB, EWISE");
    optDesc.put(PREFIX_AT + '.' + "emitNoMatch", "Emit entries that do not match the other table");
    optDesc.put(PREFIX_B + '.' + "emitNoMatch", "Emit entries that do not match the other table");

    iteratorOptions = new OptionDescriber.IteratorOptions("DotMultIterator",
        "Outer product on A and B, given AT and B. Does not sum.",
        optDesc, null);
  }


  public TwoTableIterator() {
  }

  TwoTableIterator(TwoTableIterator other) {
    this.dot = other.dot;
    this.multiplyOp = other.multiplyOp;
    this.multiplyOpOptions = other.multiplyOpOptions;
  }

  @Override
  public IteratorOptions describeOptions() {
    return iteratorOptions;
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    return validateOptionsStatic(options);
  }

  public boolean validateOptionsStatic(Map<String, String> options) {
    Map<String, String> optAT = new HashMap<>(), optB = new HashMap<>();
    new TwoTableIterator().parseOptions(options, optAT, optB);
    if (!optAT.isEmpty())
      RemoteSourceIterator.validateOptionsStatic(optAT);
    if (!optB.isEmpty())
      RemoteSourceIterator.validateOptionsStatic(optB);
    return true;
  }

  private void parseOptions(Map<String, String> options, final Map<String, String> optAT, final Map<String, String> optB) {
    for (Map.Entry<String, String> optionEntry : options.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
      if (optionKey.startsWith(PREFIX_AT + '.')) {
        String keyAfterPrefix = optionKey.substring(PREFIX_AT.length() + 1);
        switch (keyAfterPrefix) {
          case "doWholeRow":
            if (Boolean.parseBoolean(optionValue))
              log.warn("Forcing doWholeRow option on table A to FALSE. Given: " + optionValue);
            continue;
          case "emitNoMatch":
            emitNoMatchA = Boolean.parseBoolean(optionValue);
            break;
          default:
            optAT.put(keyAfterPrefix, optionValue);
            break;
        }
      } else if (optionKey.startsWith(PREFIX_B + '.')) {
        String keyAfterPrefix = optionKey.substring(PREFIX_B.length() + 1);
        switch (keyAfterPrefix) {
          case "doWholeRow":
            if (Boolean.parseBoolean(optionValue))
              log.warn("Forcing doWholeRow option on table A to FALSE. Given: " + optionValue);
            continue;
          case "emitNoMatch":
            emitNoMatchB = Boolean.parseBoolean(optionValue);
            break;
          default:
            optB.put(keyAfterPrefix, optionValue);
            break;
        }
      } else if (optionKey.startsWith("multiplyOp.opt.")) {
        String keyAfterPrefix = optionKey.substring("multiplyOp.opt.".length());
        multiplyOpOptions.put(keyAfterPrefix, optionValue);
      } else {
        switch (optionKey) {
          case "dot":
            dot = DOT_TYPE.valueOf(optionValue);
            break;
          case "multiplyOp":
            multiplyOp = GraphuloUtil.subclassNewInstance(optionValue, IMultiplyOp.class);
            break;
          default:
            log.warn("Unrecognized option: " + optionEntry);
            break;
        }
      }
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    // parse options, pass correct options to RemoteSourceIterator init()
    Map<String, String> optAT = new HashMap<>(), optB = new HashMap<>();
    parseOptions(options, optAT, optB);
    boolean dorAT = optAT.containsKey("tableName") && !optAT.get("tableName").isEmpty(),
        dorB = optB.containsKey("tableName") && !optB.get("tableName").isEmpty();
//    if (optAT.isEmpty())
//      optAT = null;
//    if (optB.isEmpty())
//      optB = null;

    if (!dorAT && !dorB && source == null) { // ~A ~B ~S
      throw new IllegalArgumentException("optAT, optB, and source cannot all be missing");
    }
    if (source == null) {
      if (dorAT) {
        remoteAT = new RemoteSourceIterator();
        remoteAT.init(null, optAT, env);
        if (dorB) {
          remoteB = new RemoteSourceIterator(); // A B ~S
          remoteB.init(null, optB, env);
        } else                                  // A ~B ~S
          remoteB = remoteAT.deepCopy(env);
      } else {
        remoteB = new RemoteSourceIterator();   // ~A B ~S
        remoteB.init(null, optB, env);
        remoteAT = remoteB.deepCopy(env);
      }
    } else { // source != null
      if (!dorAT && !dorB) {                    // ~A ~B S
        remoteAT = source;
        remoteB = source.deepCopy(env);
        remoteAT = setupRemoteSourceOptionsSKVI(remoteAT, optAT, env);
        remoteB = setupRemoteSourceOptionsSKVI(remoteB, optB, env);
      } else if (dorAT) {
        remoteAT = setup(optAT, source, env);
        if (!dorB) {
          remoteB = source;                     // A ~B S
          remoteB = setupRemoteSourceOptionsSKVI(remoteB, optB, env);
        } else {
          remoteB = setup(optB, source, env);   // A B S
        }
      } else {
        remoteB = setup(optB, source, env);
        remoteAT = source;                      // ~A B S
        remoteAT = setupRemoteSourceOptionsSKVI(remoteAT, optAT, env);
      }
    }

    multiplyOp.init(multiplyOpOptions,env);
  }

  private SortedKeyValueIterator<Key, Value> setup(
      Map<String,String> opts, SortedKeyValueIterator<Key, Value> source, IteratorEnvironment env) throws IOException {
    assert opts != null && source != null;
    String tableName = opts.get("tableName");
    SortedKeyValueIterator<Key, Value> ret;
    if (tableName != null && tableName.equals(CLONESOURCE_TABLENAME)) {
      ret = source.deepCopy(env);
      ret = setupRemoteSourceOptionsSKVI(ret, opts, env);
    } else {
      ret = new RemoteSourceIterator();
      ret.init(null, opts, env);
    }
    return ret;
  }

  private SortedKeyValueIterator<Key, Value> setupRemoteSourceOptionsSKVI(SortedKeyValueIterator<Key, Value> ret, Map<String, String> opts, IteratorEnvironment env) throws IOException {
    boolean doWholeRow = false;
    for (Map.Entry<String, String> entry : opts.entrySet()) {
      if (entry.getValue().isEmpty())
        continue;
      String key = entry.getKey();
      switch (key) {
        case "zookeeperHost":
        case "timeout":
        case "instanceName":
        case "username":
        case "password":
        case "doClientSideIterators":
          log.warn("ignoring option "+key);
          break;
        case "tableName":
          assert entry.getValue().equals(CLONESOURCE_TABLENAME);
          break;
        case "doWholeRow":
          doWholeRow = Boolean.parseBoolean(entry.getValue());
          break;
        case "rowRanges":
          SortedKeyValueIterator<Key, Value> filter = new SeekFilterIterator();
          filter.init(ret, Collections.singletonMap("rowRanges", entry.getValue()), env);
          ret = filter;
          break;
        case "colFilter":
          ret = GraphuloUtil.applyGeneralColumnFilter(entry.getValue(), ret, env);
          break;
        default:
          log.warn("Unrecognized option: " + entry);
          continue;
      }
      log.trace("Option OK: " + entry);
    }

    if (doWholeRow) {
      SortedKeyValueIterator<Key, Value> filter = new WholeRowIterator();
      filter.init(ret, Collections.<String, String>emptyMap(), env);
      ret = filter;
    }
    return ret;
  }


  private static Map.Entry<Key, Value> copyTopEntry(SortedKeyValueIterator<Key, Value> skvi) {
    final Key k = GraphuloUtil.keyCopy(skvi.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL);
    final Value v = new Value(skvi.getTopValue());
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
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
//    System.out.println("DM ori range: "+range);
//    System.out.println("DM colFamili: "+columnFamilies);
//    System.out.println("DM inclusive: " + inclusive);

    Key sk = range.getStartKey();
    // BAD: put range at beginning of row, no matter what
//    if (sk != null)
//      range = new Range(new Key(sk.getRow()), true, range.getEndKey(), range.isEndKeyInclusive());

    // if range is not infinite, see if there is a clear sign we want to restore state:
    if (sk != null && sk.getColumnFamilyData().length() == 0 && sk.getColumnQualifierData().length() == 0 && sk.getColumnVisibilityData().length() == 0
        && sk.getTimestamp() == Long.MAX_VALUE && !range.isStartKeyInclusive()) {
      // assuming that we are seeking using a key previously returned by this iterator
      // therefore go to the next row
      Key followingRowKey = sk.followingKey(PartialKey.ROW);
      if (range.getEndKey() != null && followingRowKey.compareTo(range.getEndKey()) > 0) {
        seekRange = range;
        seekColumnFamilies = columnFamilies;
        seekInclusive = inclusive;
        bottomIter = null;
        return;
      }

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

//    log.debug("remoteAT.hasTop()="+remoteAT.hasTop()+" remoteB.hasTop()="+remoteB.hasTop());
    prepNextRowMatch(/*false*/);
//    log.debug("remoteAT.hasTop()="+remoteAT.hasTop()+" remoteB.hasTop()="+remoteB.hasTop());
//    if (remoteAT.hasTop())
//      log.debug("remoteAT.getTopKey()="+remoteAT.getTopKey());
//    if (remoteB.hasTop())
//      log.debug("remoteB.getTopKey()="+remoteB.getTopKey());
  }

  private void prepNextRowMatch(/*boolean doNext*/) throws IOException {
    if ((!remoteAT.hasTop() && !remoteB.hasTop())
        || (remoteAT.hasTop() && !remoteB.hasTop() && !emitNoMatchA)
        || (!remoteAT.hasTop() && remoteB.hasTop() && !emitNoMatchB)) {
      bottomIter = null;
      return;
    }
    if (remoteAT.hasTop() && !remoteB.hasTop() && emitNoMatchA) {
      bottomIter = new PeekingIterator2<>(Iterators.singletonIterator(copyTopEntry(remoteAT)));
      remoteAT.next();
      return;
    }
    if (!remoteAT.hasTop() && remoteB.hasTop() && emitNoMatchB) {
      bottomIter = new PeekingIterator2<>(Iterators.singletonIterator(copyTopEntry(remoteB)));
      remoteB.next();
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


      PartialKey pk = null;
      switch (dot) {
        case TWOROW:
        case ONEROWB:
        case ONEROWA:
          pk = PartialKey.ROW;
          break;
        case EWISE:
        case NONE:
          pk = PartialKey.ROW_COLFAM_COLQUAL;
          break;
      }


      Watch<Watch.PerfSpan> watch = Watch.getInstance();
      do {
        int cmp = remoteAT.getTopKey().compareTo(remoteB.getTopKey(), pk);
        while (cmp != 0) {
          if (cmp < 0) {
            if (emitNoMatchA) {
              emitted = remoteAT.getTopKey();
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
              emitted = remoteB.getTopKey();
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

        switch (dot) {
          case TWOROW: {
            emitted = GraphuloUtil.keyCopy(remoteAT.getTopKey(), PartialKey.ROW);
            SortedMap<Key, Value> ArowMap = readRow(remoteAT, watch, Watch.PerfSpan.ATnext);
            SortedMap<Key, Value> BrowMap = readRow(remoteB, watch, Watch.PerfSpan.Bnext);
            multiplyOp.startRow(ArowMap, BrowMap);
            bottomIter = new PeekingIterator2<>(new CartesianDotIter(
                ArowMap.entrySet().iterator(), BrowMap, multiplyOp, false));
            break;
          }

          case ONEROWA: {
            emitted = GraphuloUtil.keyCopy(remoteAT.getTopKey(), PartialKey.ROW);
            SortedMap<Key, Value> ArowMap = readRow(remoteAT, watch, Watch.PerfSpan.ATnext);
            Iterator<Map.Entry<Key, Value>> itBonce = new SKVIRowIterator(remoteB);
            multiplyOp.startRow(ArowMap, null);
            bottomIter = new PeekingIterator2<>(new CartesianDotIter(
                itBonce, ArowMap, multiplyOp, true));
            break;
          }

          case ONEROWB: {
            emitted = GraphuloUtil.keyCopy(remoteAT.getTopKey(), PartialKey.ROW);
            Iterator<Map.Entry<Key, Value>> itAonce = new SKVIRowIterator(remoteAT);
            SortedMap<Key, Value> BrowMap = readRow(remoteB, watch, Watch.PerfSpan.Bnext);
            multiplyOp.startRow(null, BrowMap);
            bottomIter = new PeekingIterator2<>(new CartesianDotIter(
                itAonce, BrowMap, multiplyOp, false));
            break;
          }

          case EWISE: {
            emitted = remoteAT.getTopKey();
            multiplyOp.multiply(remoteAT.getTopKey().getRowData(), remoteAT.getTopKey().getColumnFamilyData(),
                remoteAT.getTopKey().getColumnQualifierData(), remoteB.getTopKey().getColumnFamilyData(),
                remoteB.getTopKey().getColumnQualifierData(), remoteAT.getTopValue(), remoteB.getTopValue());
            bottomIter = new PeekingIterator2<>(multiplyOp);
            remoteAT.next();
            remoteB.next();
            break;
          }

          case NONE: // emit nothing on collision
            bottomIter = new PeekingIterator2<>(Collections.<Map.Entry<Key, Value>>emptyIterator());
            break;
        }
      } while (bottomIter != null && !bottomIter.hasNext());
      assert hasTop();
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
   * Default is to stream through A once and iterate through B many times.
   * Pass switched as true if the two are switched.
   */
  static class CartesianDotIter implements Iterator<Map.Entry<Key, Value>> {
    private final SortedMap<Key, Value> BrowMap;
    private final boolean switched;
    private final PeekingIterator1<Map.Entry<Key, Value>> itAonce;
    private Iterator<Map.Entry<Key, Value>> itBreset;
    private final IMultiplyOp multiplyOp;

    public CartesianDotIter(Iterator<Map.Entry<Key, Value>> itAonce,  SortedMap<Key, Value> mapBreset,
                            IMultiplyOp multiplyOp, boolean switched) {
      BrowMap = mapBreset;
      this.switched = switched;
      this.itAonce = new PeekingIterator1<>(itAonce);
      this.itBreset = BrowMap.entrySet().iterator();
      this.multiplyOp = multiplyOp;
      if (itBreset.hasNext())
        prepNext();
    }

    @Override
    public boolean hasNext() {
      return multiplyOp.hasNext();
    }

    @Override
    public Map.Entry<Key, Value> next() {
      Map.Entry<Key, Value> ret = multiplyOp.next();
      if (!multiplyOp.hasNext() && itBreset.hasNext())
        prepNext();
      return ret;
    }

    private void prepNext() {
      do {
        Map.Entry<Key, Value> eA, eB = itBreset.next();
        if (!itBreset.hasNext()) {
          eA = itAonce.next();    // advance itA
          if (itAonce.hasNext())  // STOP if no more itA
            itBreset = BrowMap.entrySet().iterator();
        } else
          eA = itAonce.peek();
        if (switched)
          multiplyEntry(eB, eA);
        else
          multiplyEntry(eA, eB);
      } while (!multiplyOp.hasNext() && itBreset.hasNext());
    }

    private void multiplyEntry(Map.Entry<Key, Value> e1, Map.Entry<Key, Value> e2) {
      assert e1.getKey().getRowData().compareTo(e2.getKey().getRowData()) == 0;
      Key k1 = e1.getKey(), k2 = e2.getKey();
      multiplyOp.multiply(k1.getRowData(), k1.getColumnFamilyData(), k1.getColumnQualifierData(),
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
  public Key safeState() {
    if (bottomIter == null || bottomIter.peekSecond() != null) {
      // either we have no entries left to emit, or we need to
      // finish the row's cartesian product first (until bottomIter has one left)
      return null;
    } else {
      // the current top entry of bottomIter is the last in this cartesian product (bottomIter)
      // TWOROW/ONEROWA/ONEROWB: Save state at this row.  If reseek'd to this row, go to the next row (assume exclusive).
      // EWISE/NONE: emit the exact Key accessed.
      assert bottomIter.peekFirst() != null;
      return emitted;
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
  public TwoTableIterator deepCopy(IteratorEnvironment env) {
    TwoTableIterator copy = new TwoTableIterator(this);
    copy.remoteAT = remoteAT.deepCopy(env);
    copy.remoteB = remoteB.deepCopy(env);
    return copy;
  }
}
