package edu.mit.ll.graphulo.util;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * Iterates over a source SKVI until the row changes.
 * Does not call skvi.next() until {@link #next()} call.
 */
public class SKVIRowIterator implements Iterator<Map.Entry<Key,Value>> {
  private SortedKeyValueIterator<Key,Value> skvi;
  private byte[] row;
  private boolean matchRow;

  public SKVIRowIterator(SortedKeyValueIterator<Key, Value> skvi) {
    this.skvi = skvi;
    if (skvi.hasTop()) {
      byte[] b = skvi.getTopKey().getRowData().getBackingArray();
      row = Arrays.copyOf(b, b.length);
      matchRow = true;
    } else
      matchRow = false;
  }

  @Override
  public boolean hasNext() {
    return matchRow;
  }

  @Override
  public Map.Entry<Key, Value> next() {
    Map.Entry<Key,Value> ret = new AbstractMap.SimpleImmutableEntry<>(
        new Key(skvi.getTopKey()), new Value(skvi.getTopValue())); // maybe can eliminate Value copy

    try { // preps next entry
      skvi.next();
    } catch (IOException e) {
      throw new RuntimeException("IOException calling skvi.next()", e);
    }
    matchRow = (skvi.hasTop() &&
        Arrays.equals(row, skvi.getTopKey().getRowData().getBackingArray()));
    return ret;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Reuse this SKVIRowIterator instance after finished iterating over the current row for the next row.
   * @return True if set up for the next row; false if there are no more rows.
   * @throws IllegalStateException if called before finishing the current row.
   */
  public boolean reuseNextRow() {
    if (matchRow)
      throw new IllegalStateException("Do not reuse SKVIRowIterator until it finishes the current row: "+new String(row));
    if (!skvi.hasTop())
      return false; // cannot reuse; no more rows
    byte[] b = skvi.getTopKey().getRowData().getBackingArray();
    row = Arrays.copyOf(b, b.length);
    return matchRow = true;
  }
}
