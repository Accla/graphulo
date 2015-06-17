package edu.mit.ll.graphulo.mult;

import edu.mit.ll.graphulo.skvi.Watch;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.PeekingIterator1;
import edu.mit.ll.graphulo.util.SKVIRowIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static edu.mit.ll.graphulo.skvi.TwoTableIterator.PREFIX_AT;
import static edu.mit.ll.graphulo.skvi.TwoTableIterator.PREFIX_B;


public class CartesianRowMultiply implements RowMultiplyOp {
  private static final Logger log = LogManager.getLogger(CartesianRowMultiply.class);

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


  public enum ROWMODE {
    /** Read both rows into memory. */
    TWOROW,
    /** Read a row from A into memory and stream/iterate through columns in the row from B. */
    ONEROWA,
    /** Read a row from B into memory and stream/iterate through columns in the row from A. */
    ONEROWB
  }

  private MultiplyOp multiplyOp;
  private Map<String, String> multiplyOpOptions = new HashMap<>();
  private ROWMODE rowmode = ROWMODE.ONEROWB;
  private boolean isRowStartMultiplyOp = false;


  private void parseOptions(Map<String, String> options, final Map<String, String> optAT, final Map<String, String> optB) {
    for (Map.Entry<String, String> optionEntry : options.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
      if (optionKey.startsWith(PREFIX_AT + '.')) {
        String keyAfterPrefix = optionKey.substring(PREFIX_AT.length() + 1);
        switch (keyAfterPrefix) {
          default:
            optAT.put(keyAfterPrefix, optionValue);
            break;
        }
      } else if (optionKey.startsWith(PREFIX_B + '.')) {
        String keyAfterPrefix = optionKey.substring(PREFIX_B.length() + 1);
        switch (keyAfterPrefix) {
          default:
            optB.put(keyAfterPrefix, optionValue);
            break;
        }
      } else if (optionKey.startsWith("multiplyOp.opt.")) {
        String keyAfterPrefix = optionKey.substring("multiplyOp.opt.".length());
        multiplyOpOptions.put(keyAfterPrefix, optionValue);
      } else {
        switch (optionKey) {
          case "rowmode":
            rowmode = ROWMODE.valueOf(optionValue);
            break;
          case "multiplyOp":
            multiplyOp = GraphuloUtil.subclassNewInstance(optionValue, MultiplyOp.class);
            isRowStartMultiplyOp = multiplyOp instanceof RowStartMultiplyOp;
            break;
          default:
            log.warn("Unrecognized option: " + optionEntry);
            break;
        }
      }
    }
    if (multiplyOp == null)
      multiplyOp = new BigDecimalMultiply(); // default
  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    // parse options, pass correct options to RemoteSourceIterator init()
    Map<String, String> optAT = new HashMap<>(), optB = new HashMap<>();
    parseOptions(options, optAT, optB);
    for (Map.Entry<String, String> entry : optAT.entrySet()) {
      log.warn("unrecognized AT option: "+entry);
    }
    for (Map.Entry<String, String> entry : optB.entrySet()) {
      log.warn("unrecognized B  option: "+entry);
    }

    multiplyOp.init(multiplyOpOptions,env);
  }

  @Override
  public Iterator<Map.Entry<Key,Value>> multiplyRow(SortedKeyValueIterator<Key, Value> skviA, SortedKeyValueIterator<Key, Value> skviB) throws IOException {
    assert skviA.hasTop() && skviB.hasTop() && skviA.getTopKey().equals(skviB.getTopKey(), PartialKey.ROW);
    Watch<Watch.PerfSpan> watch = Watch.getInstance();
    
    switch (rowmode) {
      case TWOROW: {
        SortedMap<Key, Value> ArowMap = readRow(skviA, watch, Watch.PerfSpan.ATnext);
        SortedMap<Key, Value> BrowMap = readRow(skviB, watch, Watch.PerfSpan.Bnext);
        if (isRowStartMultiplyOp)
          ((RowStartMultiplyOp)multiplyOp).startRow(ArowMap, BrowMap);
        return new CartesianIterator(
            ArowMap.entrySet().iterator(), BrowMap, multiplyOp, false);
      }

      case ONEROWA: {
        SortedMap<Key, Value> ArowMap = readRow(skviA, watch, Watch.PerfSpan.ATnext);
        Iterator<Map.Entry<Key, Value>> itBonce = new SKVIRowIterator(skviB);
        if (isRowStartMultiplyOp)
          ((RowStartMultiplyOp)multiplyOp).startRow(ArowMap, null);
        return new CartesianIterator(
            itBonce, ArowMap, multiplyOp, true);
      }

      case ONEROWB: {
        Iterator<Map.Entry<Key, Value>> itAonce = new SKVIRowIterator(skviA);
        SortedMap<Key, Value> BrowMap = readRow(skviB, watch, Watch.PerfSpan.Bnext);
        if (isRowStartMultiplyOp)
          ((RowStartMultiplyOp)multiplyOp).startRow(null, BrowMap);
        return new CartesianIterator(
            itAonce, BrowMap, multiplyOp, false);
      }

      default:
        throw new AssertionError("unknown rowmode: "+rowmode);
    }
    
  }

  /**
   * Emits Cartesian product of provided iterators, passed to multiply function.
   * Default is to stream through A once and iterate through B many times.
   * Pass switched as true if the two are switched.
   */
  static class CartesianIterator implements Iterator<Map.Entry<Key, Value>> {
    private final SortedMap<Key, Value> BrowMap;
    private final boolean switched;
    private final PeekingIterator1<Map.Entry<Key, Value>> itAonce;
    private Iterator<Map.Entry<Key, Value>> itBreset;
    private final MultiplyOp multiplyOp;
    private Iterator<Map.Entry<Key, Value>> multiplyOpIterator;

    public CartesianIterator(Iterator<Map.Entry<Key, Value>> itAonce, SortedMap<Key, Value> mapBreset,
                             MultiplyOp multiplyOp, boolean switched) {
      BrowMap = mapBreset;
      this.switched = switched;
      this.itAonce = new PeekingIterator1<>(itAonce);
      this.itBreset = BrowMap.entrySet().iterator();
      this.multiplyOp = multiplyOp;
      if (itBreset.hasNext())
        prepNext();
      else
        multiplyOpIterator = Collections.emptyIterator();
    }

    @Override
    public boolean hasNext() {
      return multiplyOpIterator.hasNext();
    }

    @Override
    public Map.Entry<Key, Value> next() {
      Map.Entry<Key, Value> ret = multiplyOpIterator.next();
      if (!multiplyOpIterator.hasNext() && itBreset.hasNext())
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
        multiplyOpIterator = switched ? multiplyEntry(eB, eA) : multiplyEntry(eA, eB);
      } while (!multiplyOpIterator.hasNext() && itBreset.hasNext());
    }

    private Iterator<Map.Entry<Key, Value>> multiplyEntry(Map.Entry<Key, Value> e1, Map.Entry<Key, Value> e2) {
      assert e1.getKey().getRowData().compareTo(e2.getKey().getRowData()) == 0;
      Key k1 = e1.getKey(), k2 = e2.getKey();
      return multiplyOp.multiply(k1.getRowData(), k1.getColumnFamilyData(), k1.getColumnQualifierData(),
          k2.getColumnFamilyData(), k2.getColumnQualifierData(), e1.getValue(), e2.getValue());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

}