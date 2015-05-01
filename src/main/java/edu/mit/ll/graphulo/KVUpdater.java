package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * A class holding some {@link java.io.Serializable} state.
 * Initialized at the beginning of running. Updated from Key/Value pairs.
 * Must be capable of being sent in partial form to the client, if monitoring is enabled.
 * <p>
 * Lifecycle: init() will be called before any update(k,v) or get().
 * close() will be called before garbage collection.
 */
public interface KVUpdater<E extends Serializable> extends AutoCloseable {

  void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException;

  void update(Key k, Value v);

  E get();

}
