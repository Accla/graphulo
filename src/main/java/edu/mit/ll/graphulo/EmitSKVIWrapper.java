package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * Wrapper that bridges OutputCollector paradigm to the SKVI paradigm.
 * Implement {@link #genOutputCollector(Range, Collection, boolean, OutputCollector)} with the iterator functionality.
 */
public abstract class EmitSKVIWrapper implements EmitSKVI {
  private SortedKeyValueIterator<Key, Value> source;
  private OutputCollector ocThis;

  abstract OutputCollector genOutputCollector(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive, OutputCollector oc);

  @Override
  public void seekEmit(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive,
                       OutputCollector oc) throws IOException {
    ocThis = genOutputCollector(range, columnFamilies, inclusive, oc);
    if (source instanceof EmitSKVI) {
      ((EmitSKVI)source).seekEmit(range, columnFamilies, inclusive, ocThis);
    } else {
      // backward compatible with old SKVI sources
      source.seek(range, columnFamilies, inclusive);
      while (source.hasTop() && !ocThis.shouldStop()) {
        ocThis.collect(source.getTopKey(), source.getTopValue());
        source.next();
      }
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
  }

  // --- The following methods (except deepCopy) are only used if the downstream child of this iterator
  //     is an old SKVI not following the OutputCollector paradigm. ---

  Queue<Key> topKey = new LinkedList<>();
  Queue<Value> topValue = new LinkedList<>();
  private final OutputCollector ocTop = new OutputCollector() {
    public void collect(Key k, Value v) {topKey.add(k); topValue.add(v);}
    public boolean shouldStop() { return !topKey.isEmpty(); } // stop after 1 entry, though permitted to cache more by Queue
    public void close() {  }
  };

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    source.seek(range, columnFamilies, inclusive);
    ocThis = genOutputCollector(range, columnFamilies, inclusive, ocTop);
    if (source.hasTop())
      ocThis.collect(source.getTopKey(), source.getTopValue());
    else {
      ocThis.close();
    }
  }

  @Override
  public void next() throws IOException {
    topKey.poll();
    topValue.poll();
    if (topKey.isEmpty() && source.hasTop()) {
      source.next();
      if (source.hasTop())
        ocThis.collect(source.getTopKey(), source.getTopValue());
      else {
        ocThis.close(); //topKey = null; topValue = null;
      }
    }
  }

  @Override
  public boolean hasTop() {
    return !topKey.isEmpty();
  }

  @Override
  public Key getTopKey() {
    return topKey.peek();
  }

  @Override
  public Value getTopValue() {
    return topValue.peek();
  }

  @Override
  public EmitSKVIWrapper deepCopy(IteratorEnvironment env) {
    try {
      EmitSKVIWrapper copy = this.getClass().newInstance();
      copy.source = source.deepCopy(env);
      // initialized on seekEmit or seek call.
//      copy.ocThis
//      copy.topKey = new LinkedList<>(topKey); // shallow copy
//      copy.topValue = new LinkedList<>(topValue);
      return copy;
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Problem constructing "+this.getClass(), e);
    }
  }
}
