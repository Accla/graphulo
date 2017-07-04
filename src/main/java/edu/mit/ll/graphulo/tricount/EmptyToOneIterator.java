package edu.mit.ll.graphulo.tricount;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_VALUE;
import static edu.mit.ll.graphulo.util.GraphuloUtil.VALUE_ONE_VLONG;

public final class EmptyToOneIterator implements SortedKeyValueIterator<Key, Value> {

  private SortedKeyValueIterator<Key, Value> source;

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
  }

  @Override
  public boolean hasTop() {
    return topKey != null;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    final Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);

    source.seek(seekRange, columnFamilies, inclusive);
    findTop();

    if (range.getStartKey() != null) {
      while (hasTop() && getTopKey().equals(range.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)
          && getTopKey().getTimestamp() > range.getStartKey().getTimestamp()) {
        // the value has a more recent time stamp, so pass it up
        // log.debug("skipping "+getTopKey());
        next();
      }
      while (hasTop() && range.beforeStartKey(getTopKey()))
        next();
    }
  }

  private Key topKey;
  private Value topValue;

  private void findTop() throws IOException {
    if( !source.hasTop() ) {
      topKey = null; topValue = null;
      return;
    }
    topKey = new Key(source.getTopKey());
    topValue = source.getTopValue();
    if( topValue.getSize() == 0 )
      topValue = EMPTY_VALUE;
//    else if( topValue.equals(VALUE_ONE_VLONG) ) topValue = VALUE_ONE_VLONG;
//    else topValue = new Value(topValue); // this should never occur
    source.next();

    while( source.hasTop() && source.getTopKey().equals(topKey, PartialKey.ROW_COLFAM_COLQUAL) ) {
      if( topValue == EMPTY_VALUE && source.getTopValue().getSize() == 0 )
        topValue = VALUE_ONE_VLONG;
      source.next();
    }
  }

  @Override
  public void next() throws IOException {
    findTop();
  }

  @Override
  public Key getTopKey() {
    return topKey;
  }

  @Override
  public Value getTopValue() {
    return topValue;
  }

  @Override
  public EmptyToOneIterator deepCopy(IteratorEnvironment env) {
    final EmptyToOneIterator iter = new EmptyToOneIterator();
    iter.source = source.deepCopy(env);
    return iter;
  }
}
