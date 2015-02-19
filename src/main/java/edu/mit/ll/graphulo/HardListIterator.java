package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

/**
 * A wrapper making a list of hardcoded data into a SKVI. For testing.
 */
public class HardListIterator implements SortedKeyValueIterator<Key,Value> {
    private static SortedMap<Key,Value> allEntriesToInject;
    {
        allEntriesToInject = new TreeMap<>();
        allEntriesToInject.put(new Key(new Text("row1"), new Text("colF3"), new Text("colQ3"), System.currentTimeMillis()),
                new Value("1".getBytes()));
        allEntriesToInject.put(new Key(new Text("row4"), new Text("colF3"), new Text("colQ3"), System.currentTimeMillis()),
                new Value("1".getBytes()));
        allEntriesToInject.put(new Key(new Text("row7"), new Text("colF3"), new Text("colQ3"), System.currentTimeMillis()),
                new Value("1".getBytes()));
        allEntriesToInject = Collections.unmodifiableSortedMap(allEntriesToInject); // for safety
    }

    private PeekingIterator<Map.Entry<Key,Value>> inner;// = map.entrySet();

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        if (source != null)
            throw new IllegalArgumentException("HardListIterator does not take a parent source");
        // define behavior before seek as seek to start at negative infinity
        inner = new PeekingIterator<>( allEntriesToInject.entrySet().iterator() );
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        HardListIterator newInstance;
        try {
            newInstance = HardListIterator.class.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        newInstance.inner = new PeekingIterator<>( allEntriesToInject.tailMap(inner.peek().getKey()).entrySet().iterator() );

        return newInstance;
    }

    @Override
    public boolean hasTop() {
        return inner.hasNext();
    }

    @Override
    public void next() throws IOException {
        inner.next();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // seek to first entry inside range
        if (range.isInfiniteStartKey())
            inner = new PeekingIterator<>( allEntriesToInject.entrySet().iterator() );
        else if (range.isStartKeyInclusive())
            inner = new PeekingIterator<>( allEntriesToInject.tailMap(range.getStartKey()).entrySet().iterator() );
        else
            inner = new PeekingIterator<>( allEntriesToInject.tailMap(range.getStartKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME)).entrySet().iterator() );
    }

    @Override
    public Key getTopKey() {
        return inner.peek().getKey();
    }

    @Override
    public Value getTopValue() {
        return inner.peek().getValue();
    }
}
