package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Extends MultiIterator from Accumulo to merge together a list of iterators in sorted order.
 */
public class MyMultiIterator extends MultiIterator{

    public MyMultiIterator(List<SortedKeyValueIterator<Key, Value>> readers, boolean init) {
        super(readers, init);
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {

    }
}
