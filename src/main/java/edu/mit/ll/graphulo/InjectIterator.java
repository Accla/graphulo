package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.*;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Interleave the hardcoded values
 (row1,colF3,colQ3,1)
 (row4,colF3,colQ3,1)
 (row7,colF3,colQ3,1)
 */
public class InjectIterator extends WrappingIterator implements OptionDescriber {
    private static final Logger log = LogManager.getLogger(InjectIterator.class);


    static class HardListIterator implements SortedKeyValueIterator<Key,Value> {

        private static SortedMap<Key,Value> allEntriesToInject;
        {
            allEntriesToInject = new TreeMap<>();
            allEntriesToInject.put(new Key(new Text("row1"), new Text("colF3"), new Text("colQ3")),
                    new Value("1".getBytes()));
            allEntriesToInject.put(new Key(new Text("row4"), new Text("colF3"), new Text("colQ3")),
                    new Value("1".getBytes()));
            allEntriesToInject.put(new Key(new Text("row7"), new Text("colF3"), new Text("colQ3")),
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





    //SortedMap<Key,Value> submapToInject;

    //public InjectIterator() {}

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        InjectIterator newInstance;
        try {
            newInstance = this.getClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        newInstance.setSource(getSource().deepCopy(env));
        // COPY PARAMETERS TO NEW InjectIterator HERE
        //newInstance.submapToInject = submapToInject;

        return newInstance;
    }

//    @Override
//    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
//        super.seek(range, columnFamilies, inclusive);
//        // set submap to the first entry to inject after the start of the range
//        if (range.isInfiniteStartKey())
//            submapToInject = allEntriesToInject;
//        else if (range.isStartKeyInclusive())
//            submapToInject = allEntriesToInject.tailMap(range.getStartKey());
//        else
//            submapToInject = allEntriesToInject.tailMap(range.getStartKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL));
//
//    }

//    @Override
//    public void next() throws IOException {
//        super.next();
//    }
//
//    /** We have a top if we have a value to inject or a value from the parent iterator. */
//    @Override
//    public boolean hasTop() {
//        return !submapToInject.isEmpty() || getSource().hasTop();
//    }

//    /** True if the next value to insert is from the entries to inject.
//     *  This is true when the key to inject is less than the top key of the source.
//     */
//    protected boolean isTopInject() {
//        if (!hasTop())
//            throw new IllegalStateException("iSTopInject called but there is no top");
//        else if (submapToInject.isEmpty())
//            return false;
//        else if (!getSource().hasTop())
//            return true;
//        else {
//            // compare the values
//            Key kInject = submapToInject.firstKey();
//        }
//    }



    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        IteratorUtil.IteratorScope scope = env.getIteratorScope();
        log.debug("init on scope "+scope+( scope == IteratorUtil.IteratorScope.majc ? " fullScan="+env.isFullMajorCompaction() : "" ));
        SortedKeyValueIterator<Key,Value> side = new HardListIterator();
        side.init(null, null, env);
        env.registerSideChannel( side );

    }

    @Override
    public OptionDescriber.IteratorOptions describeOptions() {
        return new OptionDescriber.IteratorOptions("inject", "injects hard-coded entries into iterator stream.", null, null);
    }

    @Override
    public boolean validateOptions(Map<String,String> options) {
        return true;
    }
}
