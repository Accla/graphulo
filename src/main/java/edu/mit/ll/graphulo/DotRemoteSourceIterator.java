package edu.mit.ll.graphulo;

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
 */
public class DotRemoteSourceIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    private static final Logger log = LogManager.getLogger(DotRemoteSourceIterator.class);

    private IMultiplyOp multiplyOp = new BigDecimalMultiply();

    private RemoteSourceIterator remoteA, remoteBT;
    private Key nextKey;
    private Value nextValue;

    private Text ArowRow = new Text();
    private SortedMap<Key,Value> ArowMap = new TreeMap<>(new ColFamilyQualifierComparator());

    public static final String PREFIX_A = "A.";
    public static final String PREFIX_BT = "BT.";

    static final OptionDescriber.IteratorOptions iteratorOptions;
    static {
        final Map<String,String> optDesc = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
            optDesc.put(PREFIX_A+entry.getKey(), "Table A :"+entry.getValue());
        }
        for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
            optDesc.put(PREFIX_BT+entry.getKey(), "Table BT:"+entry.getValue());
        }

        iteratorOptions = new OptionDescriber.IteratorOptions("DotRemoteSourceIterator",
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
            log.warn("DotRemoteSourceIterator ignores/replaces parent source passed in init(): "+source);

        // parse options, pass correct options to RemoteSourceIterator init()
        Map<String,String> optA = new HashMap<>(), optBT = new HashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            String v = entry.getValue();
            if (key.startsWith(PREFIX_A)) {
                // A must be whole row
                String cutopt = key.substring(PREFIX_A.length());
                if (cutopt.equals("doWholeRow") && !Boolean.parseBoolean(v)) {
                    log.warn("Forcing doWholeRow option on table A to true. Given: " + v);
                    continue;
                }
                optA.put(cutopt, v);
            }
            else if (key.startsWith(PREFIX_BT)) {
                String cutopt = key.substring(PREFIX_BT.length());
                if (cutopt.equals("doWholeRow") && Boolean.parseBoolean(v)) {
                    log.warn("Forcing doWholeRow option on table BT to false. Given: " + v);
                    continue;
                }
                optBT.put(cutopt, v);
            }
            else switch(key) {

                    default:
                        log.warn("Unrecognized option: " + entry);
                        //continue;
                }
        }
        optA.put("doWholeRow","true");
        optBT.put("doWholeRow","false");
        remoteA = new RemoteSourceIterator();
        remoteBT = new RemoteSourceIterator();
        remoteA.init(null, optA, env);
        remoteBT.init(null, optBT, env);


    }


    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (!range.isInfiniteStartKey())
            range = new Range(new Key(range.getStartKey().getRow()), range.getEndKey());
        remoteA.seek(range);

        if (!remoteA.hasTop()) {
            nextKey = null;
            nextValue = null;
            return;
        }
        cacheArow();
        while (!remoteBT.hasTop()) {
            remoteA.next();
            if (!remoteA.hasTop())
                break;
            cacheArow();
        }
        if (!remoteBT.hasTop()) {
            nextKey = null;
            nextValue = null;
            return;
        }
        cacheNextEntry();
    }

    @Override
    public void next() throws IOException {
        remoteBT.next();
        while (!remoteBT.hasTop()) {
            remoteA.next();
            if (!remoteA.hasTop())
                break;
            cacheArow();
        }
        if (!remoteBT.hasTop()) {
            nextKey = null;
            nextValue = null;
            return;
        }
        cacheNextEntry();
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

    private void cacheArow() throws IOException {
        if (!remoteA.hasTop())
            throw new IllegalStateException("remoteA should hasTop(): "+remoteA);
        Key k = remoteA.getTopKey();
        k.getRow(ArowRow);
        {
            SortedMap<Key, Value> m = WholeRowIterator.decodeRow(k, remoteA.getTopValue());
            ArowMap.clear();
            ArowMap.putAll(m);
        }
        Collection<IteratorSetting.Column> c = new ArrayList<>(ArowMap.size());
        for (Key key : ArowMap.keySet()) {
            c.add(new IteratorSetting.Column(key.getColumnFamily(),key.getColumnQualifier()));
        }
        remoteBT.setFetchColumns(c);
        remoteBT.seek(INFINITE_RANGE);
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
        nextValue = multiplyOp.multiply(Avalue, BTvalue);
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
    public DotRemoteSourceIterator deepCopy(IteratorEnvironment env) {
        DotRemoteSourceIterator copy = new DotRemoteSourceIterator();
        copy.remoteA = remoteA.deepCopy(env);
        copy.remoteBT = remoteBT.deepCopy(env);
        return copy;
    }
}
