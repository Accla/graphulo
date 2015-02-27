package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A parent class for custom computation merged into a regular SKVI stack.
 * Child classes should return the iterator for their computation via initBranchIterator(options).
 */
public abstract class BranchIterator implements SortedKeyValueIterator<Key,Value> {


//    public abstract void initSetup(Map<String, String> options);
//    public abstract Pair<Key,Value> processBeforeMerge(Range seekRng, Key lastKey, Value lastValue);
//    public abstract Pair<Key,Value> processAfterMerge(Range seekRng, Key lastKey, Value lastValue);
    /**
     * Should call init() on the iterator to be setup.
     * @param options ok
     */
    public abstract SortedKeyValueIterator<Key,Value> initBranchIterator(Map<String, String> options);

    private MultiIterator multiIterator;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        //super.init(source, options, env); // sets source
        SortedKeyValueIterator<Key, Value> branchIterator = initBranchIterator(options);
        List<SortedKeyValueIterator<Key,Value>> list = new ArrayList<>(2);
        list.add(branchIterator);
        list.add(source);
        multiIterator = new MultiIterator(list, false);
    }

    @Override
    public boolean hasTop() {
        return multiIterator.hasTop();
    }

    @Override
    public void next() throws IOException {
        multiIterator.next();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        multiIterator.seek(range, columnFamilies, inclusive);
    }

    @Override
    public Key getTopKey() {
        return multiIterator.getTopKey();
    }

    @Override
    public Value getTopValue() {
        return multiIterator.getTopValue();
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        BranchIterator copy;
        try {
            copy = this.getClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("cannot construct deepCopy", e);
        }
        copy.multiIterator = multiIterator.deepCopy(env);
        return copy;
    }
}
