package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * An iterator that can reconstruct its state by signalling a special Key to emit.
 *
 */
public interface SaveStateIterator extends SortedKeyValueIterator<Key,Value> {
  /**
   * A safe state should be reconstructable from the key.
   * The safe state should be set after a seek() and next() call.
   * The returned key should be within the range passed through seek.
   * @return null if not in a safe state; non-null if can reconstruct state
   * when seek'd to the returned key.
   */
  Key safeState();
}
