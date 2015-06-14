package edu.mit.ll.graphulo.skvi;

import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Count the number of rows in a table.
 * Counts everything at once; does not return intermediate values. Could take an option to emit a value every x seconds.
 */
public class RowCountingIterator implements SortedKeyValueIterator<Key,Value> {
    private SortedKeyValueIterator<Key,Value> source;
    private Key emitKey = null;
    private Value emitValue = null;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return new RowCountingIterator();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        source.seek(range, columnFamilies, inclusive);

        long cnt = countRows();
        if (cnt != 0) {
            emitKey = range.isInfiniteStartKey() ? new Key() : (range.isStartKeyInclusive() ? range.getStartKey() : range.getStartKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL));
            emitValue = new Value(Long.toString(cnt).getBytes());
        } else {
            emitValue = null;
            emitKey = null;
        }
//    System.out.println("range "+range+" cnt "+Long.valueOf(new String(emitValue.get())) + " lastKey "+emitKey);
    }

    private long countRows() throws IOException {
        if (!source.hasTop())
            return 0;
        Text curRow = source.getTopKey().getRow();
        long cnt = 1;
        source.next();

        while (source.hasTop()) {
            if (0 != source.getTopKey().compareRow(curRow)) {
                cnt++;
                curRow = source.getTopKey().getRow(curRow);
            }
            source.next();
        }
        return cnt;
    }

    @Override
    public Key getTopKey() {
        return emitKey;
    }

    @Override
    public Value getTopValue() {
        return emitValue;
    }

    @Override
    public boolean hasTop() {
        return emitKey != null;
    }

    @Override
    public void next() throws IOException {
        emitKey = null;
        emitValue = null;
    }
}
