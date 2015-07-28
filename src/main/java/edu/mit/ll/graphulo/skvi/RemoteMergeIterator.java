package edu.mit.ll.graphulo.skvi;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Merge a RemoteSourceIterator into a regular SKVI iterator stack.
 */
public class RemoteMergeIterator extends BranchIterator {
    public static final String PREFIX_RemoteIterator = "R.";

    @Override
    public SortedKeyValueIterator<Key, Value> initBranchIterator(Map<String, String> options, IteratorEnvironment env) throws IOException {
        SortedKeyValueIterator<Key, Value> remoteIterator = new RemoteSourceIterator();
        // parse options, pass correct options to RemoteSourceIterator init()
        Map<String, String> remoteOptions = new HashMap<String, String>(options.size());
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String k = entry.getKey();
            if (k.startsWith(PREFIX_RemoteIterator))
                remoteOptions.put(k.substring(PREFIX_RemoteIterator.length()), entry.getValue());
        }
        remoteIterator.init(null, remoteOptions, env);
        return remoteIterator;
    }
}
