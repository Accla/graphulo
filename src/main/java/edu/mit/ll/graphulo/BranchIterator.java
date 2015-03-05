package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

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
    private static final Logger log = LogManager.getLogger(BranchIterator.class);

//    public abstract void initSetup(Map<String, String> options);
//    public abstract Pair<Key,Value> processBeforeMerge(Range seekRng, Key lastKey, Value lastValue);
//    public abstract Pair<Key,Value> processAfterMerge(Range seekRng, Key lastKey, Value lastValue);
    /**
     * Return the *bottom-most* iterator of the custom computation stack.
     * The resulting iterator should be initalized; should not have to call init() on the resulting iterator.
     * Can be null, but if null, which means no computation performed.
     * @param options Options passed to the BranchIterator.
     */
    public SortedKeyValueIterator<Key,Value> initBranchIterator(Map<String, String> options, IteratorEnvironment env) throws IOException {
        return null;
    }

    /**
     * Opportunity to apply an SKVI stack after merging the custom computation stack into the regular parent stack.
     * Returns parent by default.
     * @param source The MultiIterator merging the normal stack and the custom branch stack.
     * @param options Options passed to the BranchIterator.
     * @return The bottom iterator. Cannot be null. Return source if no additional computation is desired.
     */
    public SortedKeyValueIterator<Key,Value> initBranchAfterIterator(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        return source;
    }

    private SortedKeyValueIterator<Key,Value> botIterator;

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        IteratorUtil.IteratorScope scope = env.getIteratorScope();
        log.info(this.getClass() + ": init on scope " + scope + (scope == IteratorUtil.IteratorScope.majc ? " fullScan=" + env.isFullMajorCompaction() : ""));
        //log.info("BranchIterator options: "+options);
        //super.init(source, options, env); // sets source
        SortedKeyValueIterator<Key, Value> branchIterator = initBranchIterator(options, env);
        if (branchIterator == null) {
            botIterator = source;
        } else {
            List<SortedKeyValueIterator<Key, Value>> list = new ArrayList<>(2);
            list.add(branchIterator);
            list.add(source);
            botIterator = new MultiIterator(list, false);
        }
        botIterator = initBranchAfterIterator(botIterator, options, env);
        if (botIterator == null)
            throw new IllegalStateException("--some subclass returned a null bottom iterator in branchAfter--");
    }

    @Override
    public boolean hasTop() {
        return botIterator.hasTop();
    }

    @Override
    public void next() throws IOException {
        botIterator.next();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        botIterator.seek(range, columnFamilies, inclusive);
    }

    @Override
    public Key getTopKey() {
        return botIterator.getTopKey();
    }

    @Override
    public Value getTopValue() {
        return botIterator.getTopValue();
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        BranchIterator copy;
        try {
            copy = this.getClass().newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("cannot construct deepCopy", e);
        }
        copy.botIterator = botIterator.deepCopy(env);
        return copy;
    }
}
