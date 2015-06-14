package edu.mit.ll.graphulo.reducer;

import edu.mit.ll.graphulo.skvi.RemoteWriteIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Holds the logic for a commutative and associative <i>Reduce</i> operation.
 * <p>
 * A class that receives every entry passed through a {@link RemoteWriteIterator}.
 * It holds some {@link java.io.Serializable} state that is serialized and sent to the client
 * after all entries in a tablet are processed.
 * Initialized at the beginning of running. Updated from Key/Value pairs.
 * Must be capable of being sent in partial form to the client, if monitoring is enabled.
 * <p>
 * Lifecycle: init() will be called before any update(k,v) or combine(e) or get().
 * <p>
 * A BatchScan will run a reducer on every tablet that sends results to the client.
 * combine() is called at the client to combine results from each one.
 */
public interface Reducer<E extends Serializable> {

  /**
   *
   * @param options Options from the client.
   * @param env Passed from Accumulo. Null if created at the client.
   */
  void init(Map<String,String> options, IteratorEnvironment env) throws IOException;


  /** Reset reducer to its "zero" state, as if it was just init'd. Called at seek() and next() of iterators. */
  void reset() throws IOException;


  /** Update internal state with a single entry. */
  void update(Key k, Value v);

  /** Update internal state with a collection of entries. */
  void combine(E another);

  /** Signals whether the reducer has an element ready to serialize and send to the client.
   * Should always be false after reset() is called.
   */
  boolean hasTop();

  /**
   * Emit the serialized state.
   * Returns null if no value should be emitted (indicates the "zero" state).
   * This MUST return null after reset() is called and before any update() or combine() methods are called.
   */
  E get();

}
