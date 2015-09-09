package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.reducer.Reducer;

/**
 * Holds result of a OneTable or TwoTable call.
 */
public class GraphuloResult {
  private long seenEntries;
  private InputTableConfig inputForCounting;
  private Reducer reducer;

  GraphuloResult(long seenEntries, Reducer reducer) {
    this.seenEntries = seenEntries;
    this.reducer = reducer;
  }

  GraphuloResult(InputTableConfig inputForCounting, Reducer reducer) {
    this.seenEntries = -1;
    this.inputForCounting = inputForCounting;
    this.reducer = reducer;
  }

  public long getSeenEntries() {
    return seenEntries >= 0 ?
        seenEntries :
        inputForCounting.countEntries();
  }

  public Reducer getReducer() {
    return reducer;
  }
}
