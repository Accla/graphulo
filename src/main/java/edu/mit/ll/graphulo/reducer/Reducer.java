package edu.mit.ll.graphulo.reducer;

import edu.mit.ll.graphulo.skvi.RemoteWriteIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.Map;

/**
 * Holds the logic for a commutative and associative <i>Reduce</i> operation.
 * <p>
 * A class that receives every entry passed through a {@link RemoteWriteIterator}.
 * It holds a byte[] sent to the client after all entries in a tablet are processed.
 * Initialized at the beginning of running. Updated from Key/Value pairs.
 * Must be capable of being sent in partial form to the client, if monitoring is enabled.
 * <p>
 * Lifecycle: init() will be called before any update(k,v) or combine(e) or getForClient().
 * <p>
 * A BatchScan will run a reducer on every tablet that sends results to the client.
 * combine() is called at the client to combine results from each one.
 */
public interface Reducer {

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
  void combine(byte[] another);

  /** Signals whether the reducer has an element ready to serialize and send to the client.
   * Should always be false after reset() is called.
   */
  boolean hasTopForClient();

  /**
   * Emit the serialized state.
   * Returns null if no value should be emitted (indicates the "zero" state).
   * This MUST return null after reset() is called and before any update() or combine() methods are called.
   */
  byte[] getForClient();

  /** A Reducer that does nothing. */
  final class NoReducer implements Reducer {
    @Override
    public void init(Map<String,String> options, IteratorEnvironment env) throws IOException {}
    @Override
    public void reset() throws IOException {}
    @Override
    public void update(Key k, Value v) {}
    @Override
    public void combine(byte[] another) {}
    @Override
    public boolean hasTopForClient() { return false; }
    @Override
    public byte[] getForClient() { return null; }
  }

//  /**
//   * Entries sent to result tables in {@link RemoteWriteIterator} instead of the client.
//   * Use this when one needs to emit a small number of entries to result tables
//   * determined by all the entries seen in a scan.
//   * @return An iterator over entries to send to the result tables.
//   * Use {@link java.util.Collections#emptyIterator()} if none.
//   */
//  Iterator<Map.Entry<Key,Value>> getForWrite();


}
