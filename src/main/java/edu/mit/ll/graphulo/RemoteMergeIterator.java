package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;

import java.io.IOException;
import java.util.*;

/**
 * Merge a RemoteSourceIterator into a regular SKVI iterator stack.
 */
public class RemoteMergeIterator implements SortedKeyValueIterator<Key,Value> {

    private MultiIterator multiIterator;

    public RemoteMergeIterator() {}



    public static final String PREFIX_RemoteIterator = "RemoteSource.";

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        // source is already init()ed

        SortedKeyValueIterator<Key, Value> remoteIterator = new RemoteSourceIterator();
        // parse options, pass correct options to RemoteSourceIterator init()
        Map<String, String> remoteOptions = new HashMap<>(options.size());
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String k = entry.getKey();
            if (k.startsWith(PREFIX_RemoteIterator))
                remoteOptions.put(k.substring(PREFIX_RemoteIterator.length()), entry.getValue());
        }
        remoteIterator.init(null, remoteOptions, env);

        List<SortedKeyValueIterator<Key,Value>> list = new ArrayList<>(2);
        list.add(remoteIterator);
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
        RemoteMergeIterator copy = new RemoteMergeIterator();
        copy.multiIterator = multiIterator.deepCopy(env);
        return copy;
    }
}
