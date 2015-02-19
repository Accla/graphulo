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

    /**
     * Returns true if the iterator has more elements.
     *
     * @return <tt>true</tt> if the iterator has more elements.
     * @throws IllegalStateException if called before seek.
     */
    @Override
    public boolean hasTop() {
        return multiIterator.hasTop();
    }

    /**
     * Advances to the next K,V pair. Note that in minor compaction scope and in non-full major compaction scopes the iterator may see deletion entries. These
     * entries should be preserved by all iterators except ones that are strictly scan-time iterators that will never be configured for the minc or majc scopes.
     * Deletion entries are only removed during full major compactions.
     *
     * @throws java.io.IOException              if an I/O error occurs.
     * @throws IllegalStateException            if called before seek.
     * @throws java.util.NoSuchElementException if next element doesn't exist.
     */
    @Override
    public void next() throws IOException {
        multiIterator.next();
    }

    /**
     * Seeks to the first key in the Range, restricting the resulting K,V pairs to those with the specified columns. An iterator does not have to stop at the end
     * of the range. The whole range is provided so that iterators can make optimizations.
     * <p/>
     * Seek may be called multiple times with different parameters after {@link #init} is called.
     * <p/>
     * Iterators that examine groups of adjacent key/value pairs (e.g. rows) to determine their top key and value should be sure that they properly handle a seek
     * to a key in the middle of such a group (e.g. the middle of a row). Even if the client always seeks to a range containing an entire group (a,c), the tablet
     * server could send back a batch of entries corresponding to (a,b], then reseek the iterator to range (b,c) when the scan is continued.
     * <p/>
     * {@code columnFamilies} is used, at the lowest level, to determine which data blocks inside of an RFile need to be opened for this iterator. This set of data
     * blocks is also the set of locality groups defined for the given table. If no columnFamilies are provided, the data blocks for all locality groups inside of
     * the correct RFile will be opened and seeked in an attempt to find the correct start key, regardless of the startKey in the {@code range}.
     * <p/>
     * In an Accumulo instance in which multiple locality groups exist for a table, it is important to ensure that {@code columnFamilies} is properly set to the
     * minimum required column families to ensure that data from separate locality groups is not inadvertently read.
     *
     * @param range          <tt>Range</tt> of keys to iterate over.
     * @param columnFamilies <tt>Collection</tt> of column families to include or exclude.
     * @param inclusive      <tt>boolean</tt> that indicates whether to include (true) or exclude (false) column families.
     * @throws java.io.IOException      if an I/O error occurs.
     * @throws IllegalArgumentException if there are problems with the parameters.
     */
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        multiIterator.seek(range, columnFamilies, inclusive);
    }

    /**
     * Returns top key. Can be called 0 or more times without affecting behavior of next() or hasTop(). Note that in minor compaction scope and in non-full major
     * compaction scopes the iterator may see deletion entries. These entries should be preserved by all iterators except ones that are strictly scan-time
     * iterators that will never be configured for the minc or majc scopes. Deletion entries are only removed during full major compactions.
     *
     * @return <tt>K</tt>
     * @throws IllegalStateException            if called before seek.
     * @throws java.util.NoSuchElementException if top element doesn't exist.
     */
    @Override
    public Key getTopKey() {
        return multiIterator.getTopKey();
    }

    /**
     * Returns top value. Can be called 0 or more times without affecting behavior of next() or hasTop().
     *
     * @return <tt>V</tt>
     * @throws IllegalStateException            if called before seek.
     * @throws java.util.NoSuchElementException if top element doesn't exist.
     */
    @Override
    public Value getTopValue() {
        return multiIterator.getTopValue();
    }

    /**
     * Creates a deep copy of this iterator as though seek had not yet been called. init should be called on an iterator before deepCopy is called. init should
     * not need to be called on the copy that is returned by deepCopy; that is, when necessary init should be called in the deepCopy method on the iterator it
     * returns. The behavior is unspecified if init is called after deepCopy either on the original or the copy.
     *
     * @param env <tt>IteratorEnvironment</tt> environment in which iterator is being run.
     * @return <tt>SortedKeyValueIterator</tt> a copy of this iterator (with the same source and settings).
     * @throws UnsupportedOperationException if not supported.
     */
    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        RemoteMergeIterator copy = new RemoteMergeIterator();
        copy.multiIterator = multiIterator.deepCopy(env);
        return copy;
    }
}
