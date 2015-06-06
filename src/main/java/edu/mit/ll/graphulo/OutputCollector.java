package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.io.Closeable;
import java.io.IOException;

/** Similar to org.apache.hadoop.mapred.OutputCollector<Key,Value>, plus a stop signal.
 *  NOTE: Instead of signaling closure by calling collect(null,null) or something similar,
 *        the close() method from AutoCloseable will be called.  */
interface OutputCollector extends Closeable {
  /** Add an entry to output. */
  void collect(Key k, Value v) throws IOException;

  /** @return true if the calling iterator should stop emitting values. */
  boolean shouldStop();

  /** Call to signal that no more {@link #collect(Key, Value)} calls will occur and the class should clean up its state. */
  @Override
  void close() throws IOException;
}