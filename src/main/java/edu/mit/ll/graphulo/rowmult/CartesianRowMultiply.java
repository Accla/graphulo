package edu.mit.ll.graphulo.rowmult;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.skvi.Watch;
import edu.mit.ll.graphulo.util.GraphuloUtil;
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
  static SortedMap<Key, Value> readRow(SortedKeyValueIterator<Key, Value> skvi,
                                       Watch<Watch.PerfSpan> watch, Watch.PerfSpan watchtype) throws IOException {
    if (!skvi.hasTop())
      throw new IllegalStateException(skvi + " should hasTop()");
    Text thisRow = skvi.getTopKey().getRow();
    Text curRow = new Text(thisRow);
    SortedMap<Key, Value> map = new TreeMap<>();
    do {
      map.put(skvi.getTopKey(), new Value(skvi.getTopValue()));
//      watch.start(watchtype);
//      try {
        skvi.next();
//      } finally {
//        watch.stop(watchtype);
//      }
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

  public static final String ALSODOAA="alsoDoAA", ALSODOBB="alsoDoBB";

  private MultiplyOp multiplyOp;
  private Map<String, String> multiplyOpOptions = new HashMap<>();
  private ROWMODE rowmode = ROWMODE.ONEROWB;
  private boolean isRowStartMultiplyOp = false;
  private boolean alsoDoAA = false, alsoDoBB = false;


  private void parseOptions(Map<String, String> options) {
    log.debug("rowMultiplyOp options: "+options);
    for (Map.Entry<String, String> optionEntry : options.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
      if (optionKey.startsWith("multiplyOp.opt.")) {
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
          case ALSODOBB:
            alsoDoBB = Boolean.parseBoolean(optionValue);
            break;
          case ALSODOAA:
            alsoDoAA = Boolean.parseBoolean(optionValue);
            break;
          default:
            log.warn("Unrecognized option: " + optionEntry);
            break;
        }
      }
    }
    if (multiplyOp == null) {
      multiplyOp = new MathTwoScalar(); // default
      multiplyOpOptions.putAll(MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.BIGDECIMAL, ""));
    }
    if (alsoDoAA && alsoDoBB && rowmode != ROWMODE.TWOROW) {
      log.warn("Because alsoDoAA and alsoDoBB are true, forcing rowmode to " + ROWMODE.TWOROW);
      rowmode = ROWMODE.TWOROW;
    } else if (alsoDoAA && (rowmode == ROWMODE.ONEROWB)) {
      log.warn("Because alsoDoAA is true, forcing rowmode from "+ROWMODE.ONEROWB +" to " + ROWMODE.TWOROW);
      rowmode = ROWMODE.TWOROW;
    } else if (alsoDoBB && (rowmode == ROWMODE.ONEROWA)) {
      log.warn("Because alsoDoBB is true, forcing rowmode from "+ROWMODE.ONEROWA+" to " + ROWMODE.TWOROW);
      rowmode = ROWMODE.TWOROW;
    }
  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    parseOptions(options);
    multiplyOp.init(multiplyOpOptions,env);
  }

  @Override
  public Iterator<Map.Entry<Key,Value>> multiplyRow(SortedKeyValueIterator<Key, Value> skviA, SortedKeyValueIterator<Key, Value> skviB) throws IOException {
    assert skviA != null || skviB != null;
    Watch<Watch.PerfSpan> watch = null;//Watch.getInstance();

    if (skviB == null) {
      if (alsoDoAA) {
        SortedMap<Key, Value> ArowMap = readRow(skviA, watch, Watch.PerfSpan.ATnext);
        if (isRowStartMultiplyOp)
          if (!((RowStartMultiplyOp)multiplyOp).startRow(ArowMap, null, false)) {
            return Collections.emptyIterator();
          }
        return new CartesianIterator(ArowMap.entrySet().iterator(), ArowMap, multiplyOp, false);
      } else {
        return Iterators.singletonIterator(GraphuloUtil.copyTopEntry(skviA));
      }
    }
    if (skviA == null) {
      if (alsoDoBB) {
        SortedMap<Key, Value> BrowMap = readRow(skviB, watch, Watch.PerfSpan.ATnext);
        if (isRowStartMultiplyOp)
          if (!((RowStartMultiplyOp)multiplyOp).startRow(null, BrowMap, false)) {
            return Collections.emptyIterator();
          }
        return new CartesianIterator(BrowMap.entrySet().iterator(), BrowMap, multiplyOp, false);
      } else {
        return Iterators.singletonIterator(GraphuloUtil.copyTopEntry(skviB));
      }
    }


    assert skviA.hasTop() && skviB.hasTop() && skviA.getTopKey().equals(skviB.getTopKey(), PartialKey.ROW);
    
    switch (rowmode) {
      case TWOROW: {
        SortedMap<Key, Value> ArowMap = readRow(skviA, watch, Watch.PerfSpan.ATnext);
        SortedMap<Key, Value> BrowMap = readRow(skviB, watch, Watch.PerfSpan.Bnext);
        if (isRowStartMultiplyOp)
          if (!((RowStartMultiplyOp)multiplyOp).startRow(ArowMap, BrowMap, true))
            return Collections.emptyIterator();

        Iterator<Map.Entry<Key,Value>> itAB = new CartesianIterator(ArowMap.entrySet().iterator(), BrowMap, multiplyOp, false);
        if (alsoDoAA && alsoDoBB) {
          Iterator<Map.Entry<Key,Value>> itAA = new CartesianIterator(ArowMap.entrySet().iterator(), ArowMap, multiplyOp, false),
            itBB = new CartesianIterator(BrowMap.entrySet().iterator(), BrowMap, multiplyOp, false);
          return Iterators.concat(itAB, itAA, itBB);
        } else if (alsoDoAA) {
          Iterator<Map.Entry<Key,Value>> itAA = new CartesianIterator(ArowMap.entrySet().iterator(), ArowMap, multiplyOp, false);
          return Iterators.concat(itAB, itAA);
        } else if (alsoDoBB) {
          Iterator<Map.Entry<Key,Value>> itBB = new CartesianIterator(BrowMap.entrySet().iterator(), BrowMap, multiplyOp, false);
          return Iterators.concat(itAB, itBB);
        } else
          return itAB;
      }

      case ONEROWA: {
        assert !alsoDoBB;
        SortedMap<Key, Value> ArowMap = readRow(skviA, watch, Watch.PerfSpan.ATnext);
        Iterator<Map.Entry<Key, Value>> itBonce = new SKVIRowIterator(skviB);
        if (isRowStartMultiplyOp)
          if (!((RowStartMultiplyOp)multiplyOp).startRow(ArowMap, null, true)) {
            // skip rest of row at skviB
            ArowMap = null;
            while (itBonce.hasNext())
              itBonce.next();
            return Collections.emptyIterator();
          }

        Iterator<Map.Entry<Key,Value>> itAB = new CartesianIterator(itBonce, ArowMap, multiplyOp, true);
        if (alsoDoAA) {
          Iterator<Map.Entry<Key,Value>> itAA = new CartesianIterator(ArowMap.entrySet().iterator(), ArowMap, multiplyOp, false);
          return Iterators.concat(itAB, itAA);
        } else
          return itAB;
      }

      case ONEROWB: {
        assert !alsoDoAA;
        Iterator<Map.Entry<Key, Value>> itAonce = new SKVIRowIterator(skviA);
        SortedMap<Key, Value> BrowMap = readRow(skviB, watch, Watch.PerfSpan.Bnext);
        if (isRowStartMultiplyOp)
          if (!((RowStartMultiplyOp)multiplyOp).startRow(null, BrowMap, true)) {
            // skip rest of row at skviA
            BrowMap = null;
            while (itAonce.hasNext())
              itAonce.next();
            return Collections.emptyIterator();
          }

        Iterator<Map.Entry<Key, Value>> itAB = new CartesianIterator(itAonce, BrowMap, multiplyOp, false);
        if (alsoDoBB) {
          Iterator<Map.Entry<Key,Value>> itBB = new CartesianIterator(BrowMap.entrySet().iterator(), BrowMap, multiplyOp, false);
          return Iterators.concat(itAB, itBB);
        } else
          return itAB;
      }

      default:
        throw new AssertionError("unknown rowmode: "+rowmode);
    }
    
  }

}