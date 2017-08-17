package edu.mit.ll.graphulo.util;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

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
      byte[] b = skvi.getTopKey().getRowData().toArray();
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
        Arrays.equals(row, skvi.getTopKey().getRowData().toArray()));
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
      throw new IllegalStateException("Do not reuse SKVIRowIterator until it finishes the current row: "+new String(row, StandardCharsets.UTF_8));
    if (!skvi.hasTop())
      return false; // cannot reuse; no more rows
    byte[] b = skvi.getTopKey().getRowData().toArray();
    row = Arrays.copyOf(b, b.length);
    return matchRow = true;
  }

  /**
   * Read a row from skvi into an in-memory SortedMap, advancing the skvi to the start of the next row.
   * No effect if <pre>!skvi.hasTop()</pre>.
   */
  public static SortedMap<Key,Value> readRowIntoMap(SortedKeyValueIterator<Key,Value> skvi) {
    SortedMap<Key,Value> map = new TreeMap<>();
    SKVIRowIterator rowIter = new SKVIRowIterator(skvi);
    while (rowIter.hasNext()) {
      Map.Entry<Key, Value> entry = rowIter.next();
      map.put(entry.getKey(), entry.getValue());
    }
    return map;
  }

  /**
   * Dump the current row, so that the top key is at the next row or there are no more rows.
   */
  public static void dumpRow(SortedKeyValueIterator<Key,Value> skvi) throws IOException {
    Text thisRow = skvi.getTopKey().getRow();
    Text curRow = new Text(thisRow);
    do {
      skvi.next();
    } while (skvi.hasTop() && skvi.getTopKey().getRow(curRow).equals(thisRow));
  }
}
