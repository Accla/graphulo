package edu.mit.ll;

import edu.mit.ll.graphulo.reducer.Reducer;

/**
 * Holds result of a OneTable or TwoTable call.
 */
public class GraphuloResult {
  private long numEntriesAtRemoteWriteIterator;
  private Reducer reducer;

  public GraphuloResult(long numEntriesAtRemoteWriteIterator, Reducer reducer) {
    this.numEntriesAtRemoteWriteIterator = numEntriesAtRemoteWriteIterator;
    this.reducer = reducer;
  }

  public long getNumEntriesAtRemoteWriteIterator() {
    return numEntriesAtRemoteWriteIterator;
  }

  public Reducer getReducer() {
    return reducer;
  }
}
