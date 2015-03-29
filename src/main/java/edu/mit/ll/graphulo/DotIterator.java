package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.mult.BigDecimalMultiply;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * The dot product part of table-table multiplication.
 * Todo: make this generic by accepting a whole row A source and a B source, instead of specifically RemoteSourceIterators. Configure options outside. Subclass RemoteSotMultIterator.
 */
public class DotIterator implements SortedKeyValueIterator<Key, Value>, OptionDescriber {
  private static final Logger log = LogManager.getLogger(DotIterator.class);

  private IMultiplyOp multiplyOp = new BigDecimalMultiply();
  private RemoteSourceIterator remoteA, remoteBT;

  private SortedMap<Key, Value> ArowMap;
  private Collection<IteratorSetting.Column> ArowMapCols;
  private PeekingIterator<Map.Entry<Key, Value>> bottomIter; // = new TreeMap<>(new ColFamilyQualifierComparator());

  public static final String PREFIX_A = "A";
  public static final String PREFIX_BT = "BT";
  private static final Range INFINITE_RANGE = new Range();

  static final OptionDescriber.IteratorOptions iteratorOptions;

  static {
    final Map<String, String> optDesc = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
      optDesc.put(PREFIX_A + entry.getKey(), "Table A :" + entry.getValue());
    }
    for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
      optDesc.put(PREFIX_BT + entry.getKey(), "Table BT:" + entry.getValue());
    }

    iteratorOptions = new OptionDescriber.IteratorOptions("DotMultIterator",
        "Dot product on rows from Accumulo tables A and BT. Does not sum.",
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
    Map<String, String> optA = new HashMap<>(), optBT = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(PREFIX_A))
        optA.put(key.substring(PREFIX_A.length()), entry.getValue());
      else if (key.startsWith(PREFIX_BT))
        optBT.put(key.substring(PREFIX_BT.length()), entry.getValue());
      else switch (key) {

          default:
            throw new IllegalArgumentException("unknown option: " + entry);
        }
    }
    return RemoteSourceIterator.validateOptionsStatic(optA) &&
        RemoteSourceIterator.validateOptionsStatic(optBT);
  }


  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    if (source != null)
      log.warn("DotMultIterator ignores/replaces parent source passed in init(): " + source);

    // parse options, pass correct options to RemoteSourceIterator init()
    Map<String, String> optA = null, optBT = null;
    {
      Map<String, Map<String, String>> prefixMap = GraphuloUtil.splitMapPrefix(options);
      for (Map.Entry<String, Map<String, String>> prefixEntry : prefixMap.entrySet()) {
        String prefix = prefixEntry.getKey();
        Map<String, String> entryMap = prefixEntry.getValue();
        switch (prefix) {
          case PREFIX_A: {
            String v = entryMap.remove("doWholeRow");
            if (v != null && !Boolean.parseBoolean(v)) {
              log.warn("Forcing doWholeRow option on table A to true. Given: " + v);
            }
            optA = entryMap;
            optA.put("doWholeRow", "true");
            break;
          }
          case PREFIX_BT: {
            String v = entryMap.remove("doWholeRow");
            if (v != null && !Boolean.parseBoolean(v)) {
              log.warn("Forcing doWholeRow option on table BT to TRUE. Given: " + v);
            }
            optBT = entryMap;
            optBT.put("doWholeRow", "true");
            break;
          }
          default:
            for (Map.Entry<String, String> entry : entryMap.entrySet()) {
              log.warn("Unrecognized option: " + prefix + '.' + entry);
            }
            break;
        }
      }
      if (optA == null) optA = new HashMap<>();
      if (optBT == null) optBT = new HashMap<>();
    }

    remoteA = new RemoteSourceIterator();
    remoteBT = new RemoteSourceIterator();
    remoteA.init(null, optA, env);
    remoteBT.init(null, optBT, env);

    log.trace("DotMultIterator init() ok");
  }


  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    Range rA, rBT;
    // if range is in the middle of a row, seek A to the beginning of the row and B to the correct location
    if (range.isInfiniteStartKey()) {
      rBT = INFINITE_RANGE;
      if (range.isInfiniteStopKey())
        rA = INFINITE_RANGE;
      else
        rA = new Range(null, range.getEndKey().getRow());
    } else {
      rBT = new Range(range.getStartKey().getColumnQualifier(), range.isStartKeyInclusive(), null, false);
      // Use special start key to work around seek() method of RowEncodingIterator
      Key startK = new Key(range.getStartKey().getRow());
      if (!range.isStartKeyInclusive())
        startK.setTimestamp(Long.MAX_VALUE - 1);
      if (range.isInfiniteStopKey())
        rA = new Range(startK, range.isStartKeyInclusive(), null, false);
      else
        rA = new Range(startK, range.isStartKeyInclusive(), new Key(range.getEndKey().getRow()), true);
    }
    log.debug("seek range: " + range);
    log.debug("rA   range: " + rA);
    log.debug("rBT  range: " + rBT);

    Watch.instance.start(Watch.PerfSpan.Anext);
    try {
      remoteA.seek(rA);
    } finally {
      Watch.instance.stop(Watch.PerfSpan.Anext);
    }
    // choosing not to handle case where we end in the middle of a row; allowed to return entries beyond the seek range
    if (!remoteA.hasTop()) {
      bottomIter = null;
      return;
    }
    // Cache A immediately so that we can set fetchColumns()
    Watch.instance.start(Watch.PerfSpan.ArowDecode);
    try {
      ArowMap = WholeRowIterator.decodeRow(remoteA.getTopKey(), remoteA.getTopValue());
    } finally {
      Watch.instance.stop(Watch.PerfSpan.ArowDecode);
    }
    Iterator<Map.Entry<Key, Value>> ArowMapIter = ArowMap.entrySet().iterator();
    ArowMapCols = new ArrayList<>(ArowMap.size());
    for (Key key : ArowMap.keySet()) {
      ArowMapCols.add(new IteratorSetting.Column(key.getColumnFamily(), key.getColumnQualifier()));
    }

    Watch.instance.start(Watch.PerfSpan.BTnext);
    try {
      remoteBT.setFetchColumns(ArowMapCols);
      remoteBT.seek(rBT);
    } finally {
      Watch.instance.stop(Watch.PerfSpan.BTnext);
    }
    if (!remoteBT.hasTop()) {
      Watch.instance.start(Watch.PerfSpan.Anext);
      try {
        remoteA.next();
      } finally {
        Watch.instance.stop(Watch.PerfSpan.Anext);
      }
      if (!remoteA.hasTop()) {
        bottomIter = null;
        return;
      }
      Watch.instance.start(Watch.PerfSpan.BTnext);
      try {
        remoteBT.seek(INFINITE_RANGE);
      } finally {
        Watch.instance.stop(Watch.PerfSpan.BTnext);
      }
      if (!remoteBT.hasTop()) {
        bottomIter = null;
        return;
      }
    }
    assert remoteA.hasTop();
    assert remoteBT.hasTop();

    Iterator<Map.Entry<Key, Value>> BTrowMapIter;



    Watch.instance.start(Watch.PerfSpan.BTrowDecode);
    try {
      BTrowMapIter = WholeRowIterator.decodeRow(remoteBT.getTopKey(), remoteBT.getTopValue()).entrySet().iterator();
    } finally {
      Watch.instance.stop(Watch.PerfSpan.BTrowDecode);
    }
    bottomIter = new PeekingIterator<>(new DotIter(ArowMapIter, BTrowMapIter, multiplyOp));

    prepNextBottomIter();
  }

  private void prepNextBottomIter() throws IOException {
    while (!bottomIter.hasNext()) {
      // advance BT
      Watch.instance.start(Watch.PerfSpan.BTnext);
      try {
        remoteBT.next();
      } finally {
        Watch.instance.stop(Watch.PerfSpan.BTnext);
      }
      if (!remoteBT.hasTop()) {
        // advance A
        Watch.instance.start(Watch.PerfSpan.Anext);
        try {
          remoteA.next();
        } finally {
          Watch.instance.stop(Watch.PerfSpan.Anext);
        }
        if (!remoteA.hasTop()) {
          // no more entries
          bottomIter = null;
          break;
        }
        assert remoteA.hasTop();
        Watch.instance.start(Watch.PerfSpan.ArowDecode);
        try {
          ArowMap = WholeRowIterator.decodeRow(remoteA.getTopKey(), remoteA.getTopValue());
        } finally {
          Watch.instance.stop(Watch.PerfSpan.ArowDecode);
        }
        ArowMapCols = new ArrayList<>(ArowMap.size());
        // reset BT
        Watch.instance.start(Watch.PerfSpan.BTnext);
        try {
          remoteBT.setFetchColumns(ArowMapCols);
          remoteBT.seek(INFINITE_RANGE);
        } finally {
          Watch.instance.stop(Watch.PerfSpan.BTnext);
        }
        assert remoteBT.hasTop(); // would have been caught above
      }
      //BT has top
      Iterator<Map.Entry<Key, Value>> ArowMapIter = ArowMap.entrySet().iterator(), BTrowMapIter;
      Watch.instance.start(Watch.PerfSpan.BTrowDecode);
      try {
        BTrowMapIter = WholeRowIterator.decodeRow(remoteBT.getTopKey(), remoteBT.getTopValue()).entrySet().iterator();
      } finally {
        Watch.instance.stop(Watch.PerfSpan.BTrowDecode);
      }
      bottomIter = new PeekingIterator<>(new DotIter(ArowMapIter, BTrowMapIter, multiplyOp));
    }
    // if bottomIter == null, then done.
  }

  static class DotIter implements Iterator<Map.Entry<Key, Value>> {
    private Iterator<Map.Entry<Key, Value>> i1, i2;
    private static Comparator<Key> comparator = new ColFamilyQualifierComparator();
    private IMultiplyOp multiplyOp;
    private Map.Entry<Key, Value> nextEntry;

    Map.Entry<Key, Value> multiplyEntry(Map.Entry<Key, Value> e1, Map.Entry<Key, Value> e2) {
      assert comparator.compare(e1.getKey(), e2.getKey()) == 0;
      Key k1 = e1.getKey(), k2 = e2.getKey();
      final Key k = new Key(k1.getRow(), k1.getColumnFamily(), k2.getRow(), System.currentTimeMillis());
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

    public DotIter(Iterator<Map.Entry<Key, Value>> iter1,
                   Iterator<Map.Entry<Key, Value>> iter2,
                   IMultiplyOp multiplyOp) {
      this.i1 = iter1;
      this.i2 = iter2;
      this.multiplyOp = multiplyOp;
      //            this.comparator = comparator;
      nextEntry = prepareNext();
    }

    @Override
    public boolean hasNext() {
      return nextEntry != null;
    }

    @Override
    public Map.Entry<Key, Value> next() {
      Map.Entry<Key, Value> ret = nextEntry;
      nextEntry = prepareNext();
      return ret;
    }

    private Map.Entry<Key, Value> prepareNext() {
      if (!i1.hasNext() || !i2.hasNext()) {
        return null;
      }
      Map.Entry<Key, Value> e1 = i1.next();
      Map.Entry<Key, Value> e2 = i2.next();
      int cmp = comparator.compare(e1.getKey(), e2.getKey());
      while (cmp < 0 && i1.hasNext() || cmp > 0 && i2.hasNext()) {
        if (cmp < 0) {
          e1 = i1.next();
        } else if (cmp > 0) {
          e2 = i2.next();
        }
        cmp = comparator.compare(e1.getKey(), e2.getKey());
      }
      return cmp == 0 ? multiplyEntry(e1, e2) : null;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }


  @Override
  public void next() throws IOException {
    bottomIter.next();
    prepNextBottomIter();
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
    copy.remoteA = remoteA.deepCopy(env);
    copy.remoteBT = remoteBT.deepCopy(env);
    return copy;
  }
}
