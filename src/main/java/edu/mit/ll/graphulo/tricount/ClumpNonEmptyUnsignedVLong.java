package edu.mit.ll.graphulo.tricount;

import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_KEY;
import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_VALUE;

public final class ClumpNonEmptyUnsignedVLong implements SortedKeyValueIterator<Key, Value> {

  private SortedKeyValueIterator<Key, Value> source;

  private Key lastKey;
  private boolean afterLastKey;
  private long tri;

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
  }

  @Override
  public boolean hasTop() {
    return tri != 0 || source.hasTop();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    source.seek(range, columnFamilies, inclusive);
    lastKey = range.isInfiniteStartKey() ? EMPTY_KEY : range.getStartKey();
    afterLastKey = !range.isInfiniteStartKey() && !range.isStartKeyInclusive();
    tri = 0;
    findTop();
  }

  private void findTop() throws IOException {
    while( source.hasTop() && !source.getTopValue().equals(EMPTY_VALUE) ) {
      afterLastKey = false;
      lastKey = source.getTopKey();
      tri += GraphuloUtil.readUnsignedVLong(source.getTopValue().get());
      source.next();
    }
  }

  @Override
  public void next() throws IOException {
    lastKey = source.getTopKey();
    afterLastKey = true;
    if( source.hasTop() )
      source.next();
    else
      tri = 0;
    findTop();
  }

  @Override
  public Key getTopKey() {
    if( source.hasTop() )
      return source.getTopKey();
    else
      return afterLastKey ? lastKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL) : lastKey;
  }

  @Override
  public Value getTopValue() {
    if( source.hasTop() )
      return source.getTopValue();
    else
      return new Value(GraphuloUtil.writeVUnsignedLong(tri));
  }

  @Override
  public ClumpNonEmptyUnsignedVLong deepCopy(IteratorEnvironment env) {
    final ClumpNonEmptyUnsignedVLong iter = new ClumpNonEmptyUnsignedVLong();
    iter.source = source.deepCopy(env);
    return iter;
  }
}
