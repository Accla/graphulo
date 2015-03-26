package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.mult.BigDecimalMultiply;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * The multiply part of table-table multiplication.
 * Todo: make this generic by accepting a whole row A source and a B source, instead of specifically RemoteSourceIterators. Configure options outside. Subclass RemoteSotMultIterator.
 */
public class DotMultIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    private static final Logger log = LogManager.getLogger(DotMultIterator.class);

    private IMultiplyOp multiplyOp = new BigDecimalMultiply();

    private RemoteSourceIterator remoteA, remoteBT;
    private Key nextKey;
    private Value nextValue;

    private Text ArowRow = new Text();
    private SortedMap<Key,Value> ArowMap = new TreeMap<>(new ColFamilyQualifierComparator());

    public static final String PREFIX_A = "A";
    public static final String PREFIX_BT = "BT";

    static final OptionDescriber.IteratorOptions iteratorOptions;
    static {
        final Map<String,String> optDesc = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
            optDesc.put(PREFIX_A+entry.getKey(), "Table A :"+entry.getValue());
        }
        for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
            optDesc.put(PREFIX_BT+entry.getKey(), "Table BT:"+entry.getValue());
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
        Map<String,String> optA = new HashMap<>(), optBT = new HashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(PREFIX_A))
                optA.put(key.substring(PREFIX_A.length()), entry.getValue());
            else if (key.startsWith(PREFIX_BT))
                optBT.put(key.substring(PREFIX_BT.length()), entry.getValue());
            else switch(key) {

                    default:
                        throw new IllegalArgumentException("unknown option: "+entry);
                }
        }
        return RemoteSourceIterator.validateOptionsStatic(optA) &&
                RemoteSourceIterator.validateOptionsStatic(optBT);
    }


    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        if (source != null)
            log.warn("DotMultIterator ignores/replaces parent source passed in init(): "+source);

        // parse options, pass correct options to RemoteSourceIterator init()
        Map<String,String> optA=null, optBT=null;
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
                        if (v != null && Boolean.parseBoolean(v)) {
                            log.warn("Forcing doWholeRow option on table BT to false. Given: " + v);
                        }
                        optBT = entryMap;
                        optBT.put("doWholeRow", "false");
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
        }
        else {
            rBT = new Range(range.getStartKey().getColumnQualifier(), range.isStartKeyInclusive(), null, false);
            // Use special start key to work around seek() method of RowEncodingIterator
            Key startK = new Key(range.getStartKey().getRow(), Long.MAX_VALUE-1);
            if (range.isInfiniteStopKey())
                rA = new Range(startK, null);
            else
                rA = new Range(startK, new Key(range.getEndKey().getRow()));

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
            nextKey = null;
            nextValue = null;
            return;
        }
        cacheArow(false);

        Watch.instance.start(Watch.PerfSpan.BTnext);
        try {
            remoteBT.seek(rBT);
        } finally {
            Watch.instance.stop(Watch.PerfSpan.BTnext);
        }
        while (!remoteBT.hasTop()) {
            Watch.instance.start(Watch.PerfSpan.Anext);
            try {
                remoteA.next();
            } finally {
                Watch.instance.stop(Watch.PerfSpan.Anext);
            }
            if (!remoteA.hasTop())
                break;
            cacheArow(true);
        }
        if (!remoteBT.hasTop()) {
            nextKey = null;
            nextValue = null;
            return;
        }
        cacheNextEntry();
        if (hasTop())
            log.debug("prepared next entry "+getTopKey()+" ==> "
                    +getTopValue());
        else {
            log.info("hasTop() == false");
//            Watch.instance.print("tid " + Thread.currentThread().getName() + ": ");
//            Watch.instance.resetAll();
        }
    }

    @Override
    public void next() throws IOException {
        Watch.instance.start(Watch.PerfSpan.BTnext);
        try {
            remoteBT.next();
        } finally {
            Watch.instance.stop(Watch.PerfSpan.BTnext);
        }

        while (!remoteBT.hasTop()) {
            Watch.instance.start(Watch.PerfSpan.Anext);
            try {
                remoteA.next();
            } finally {
                Watch.instance.stop(Watch.PerfSpan.Anext);
            }

            if (!remoteA.hasTop())
                break;
            cacheArow(true);
        }
        if (!remoteBT.hasTop()) {
            nextKey = null;
            nextValue = null;
            return;
        }
        cacheNextEntry();
        if (hasTop())
            log.trace("prepared next entry " + getTopKey() + " ==> "
                + getTopValue());
        else {
            log.info("hasTop() == false");
//            Watch.instance.print("tid " + Thread.currentThread().getName() + ": ");
//            Watch.instance.resetAll();
        }
    }

    /** Compare only the column family and column qualifier. */
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
    private static final Range INFINITE_RANGE = new Range();

    private void cacheArow(boolean seekBT) throws IOException {
        if (!remoteA.hasTop())
            throw new IllegalStateException("remoteA should hasTop(): "+remoteA);
        Key k = remoteA.getTopKey();
        Watch.instance.start(Watch.PerfSpan.ArowDecode);
        Collection<IteratorSetting.Column> c;
        try {
            k.getRow(ArowRow);
            {
                SortedMap<Key, Value> m = WholeRowIterator.decodeRow(k, remoteA.getTopValue());
                ArowMap.clear();
                ArowMap.putAll(m);
            }
            c = new ArrayList<>(ArowMap.size());
            for (Key key : ArowMap.keySet()) {
                c.add(new IteratorSetting.Column(key.getColumnFamily(), key.getColumnQualifier()));
            }
        } finally {
            Watch.instance.stop(Watch.PerfSpan.ArowDecode);
        }

        remoteBT.setFetchColumns(c);
        Watch.instance.start(Watch.PerfSpan.BTnext);
        try {
            if (seekBT)
                remoteBT.seek(INFINITE_RANGE);
        } finally {
            Watch.instance.stop(Watch.PerfSpan.BTnext);
        }
    }

    private void cacheNextEntry() {
        if (!remoteBT.hasTop() || ArowMap == null || ArowRow == null)
            throw new IllegalStateException("remoteBT should hasTop(): "+remoteBT);
        Key BTkey = remoteBT.getTopKey();
        Value BTvalue = remoteBT.getTopValue();
        Value Avalue = ArowMap.get(BTkey);
        if (Avalue == null)
            throw new IllegalStateException("every entry from BTremote should contain a matching (CF,CQ) " +
                    "to the current row of A. BTkey="+BTkey+"; ArowRow="+ ArowRow +"; ArowMap="+ArowMap);
        Watch.instance.start(Watch.PerfSpan.Multiply);
        try {
            nextValue = multiplyOp.multiply(Avalue, BTvalue);
        } finally {
            Watch.instance.stop(Watch.PerfSpan.Multiply);
        }
        nextKey = new Key(ArowRow, BTkey.getColumnFamily(), BTkey.getRow(), System.currentTimeMillis());
    }

    @Override
    public boolean hasTop() {
        assert !((nextValue == null) ^ (nextKey == null));
        return nextValue != null;
    }

    @Override
    public Key getTopKey() {
        return nextKey;
    }

    @Override
    public Value getTopValue() {
        return nextValue;
    }

    @Override
    public DotMultIterator deepCopy(IteratorEnvironment env) {
        DotMultIterator copy = new DotMultIterator();
        copy.remoteA = remoteA.deepCopy(env);
        copy.remoteBT = remoteBT.deepCopy(env);
        return copy;
    }
}
