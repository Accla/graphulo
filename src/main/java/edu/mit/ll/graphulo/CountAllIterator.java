package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Counts all entries from its source iterators and emits a single entry containing their count.
 * The Key of the emitted entry is right after the (row,colF,colQ) of the seek range start key; other fields empty.
 */
public class CountAllIterator implements SortedKeyValueIterator<Key,Value> {
  private SortedKeyValueIterator<Key,Value> source;
  private Key emitKey = null;
  private Value emitValue = null;

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    return new CountAllIterator();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    source.seek(range, columnFamilies, inclusive);
    long cnt = countAll();
    emitValue = new Value(Long.toString(cnt).getBytes());
    emitKey = range.getStartKey() == null ? new Key("a") : range.getStartKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL);
//    System.out.println("range "+range+" cnt "+Long.valueOf(new String(emitValue.get())) + " lastKey "+emitKey);
  }

  /**
   * Reads all entries from the parent iterator, computing the value you want to emit.
   * Example given is summing the Values of parent entries, interpreted as Longs.
   */
  private long countAll() throws IOException {
    long cnt = 0l;
    while (source.hasTop()) {
      cnt++;
      source.next();
    }
    return cnt;
  }

  @Override
  public Key getTopKey() {
    return emitKey;
  }

  @Override
  public Value getTopValue() {
    return emitValue;
  }

  @Override
  public boolean hasTop() {
    return emitKey != null;
  }

  @Override
  public void next() throws IOException {
    emitKey = null;
    emitValue = null;
  }
}
