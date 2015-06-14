package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Limits the Ranges seeked to to a certain subset.
 */
public class SeekFilterIterator implements SortedKeyValueIterator<Key, Value>, OptionDescriber {
  private static final Logger log = LogManager.getLogger(RemoteSourceIterator.class);

  SortedKeyValueIterator<Key, Value> source;
  private SortedSet<Range> rowRanges = new TreeSet<>(Collections.singleton(new Range()));
  /**
   * Holds the current range we are scanning.
   * Goes through the part of ranges after seeking to the beginning of the seek() clip.
   */
  private Iterator<Range> rowRangeIterator;

  /**
   * The range given by seek. Clip to this range.
   */
  private Range seekRange;
  private Collection<ByteSequence> columnFamilies;
  private boolean inclusive;

  /**
   * Call init() after construction.
   */
  public SeekFilterIterator() {
  }

  SeekFilterIterator(SeekFilterIterator other) {
    other.rowRanges = rowRanges;
  }

  static final IteratorOptions iteratorOptions;

  static {
    Map<String, String> optDesc = new LinkedHashMap<>();
    optDesc.put("rowRanges", "Row ranges to scan for remote Accumulo table, Matlab syntax. (default ':,' all)");
    iteratorOptions = new IteratorOptions("SeekFilterIterator",
        "Intersects seek ranges with the option given, passing the reduced range to the parent.",
        Collections.unmodifiableMap(optDesc), null);
  }

  @Override
  public IteratorOptions describeOptions() {
    return iteratorOptions;
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    return validateOptionsStatic(options);
  }

  public static boolean validateOptionsStatic(Map<String, String> options) {
    new SeekFilterIterator().parseOptions(options);
    return true;
  }

  private void parseOptions(Map<String, String> map) {
    for (Map.Entry<String, String> entry : map.entrySet()) {
      if (entry.getValue().isEmpty())
        continue;
      switch (entry.getKey()) {
        case "rowRanges":
          rowRanges = RemoteSourceIterator.parseRanges(entry.getValue());
          break;
        default:
          log.warn("Unrecognized option: " + entry);
          continue;
      }
      log.trace("Option OK: " + entry);
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> map, IteratorEnvironment iteratorEnvironment) throws IOException {
    this.source = source;
    parseOptions(map);
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    seekRange = range;
    this.columnFamilies = columnFamilies;
    this.inclusive = inclusive;
    rowRangeIterator = RemoteSourceIterator.getFirstRangeStarting(
        new PeekingIterator1<>(rowRanges.iterator()), range);
//    sourceIterator = new PeekingIterator1<>(java.util.Collections.<Map.Entry<Key, Value>>emptyIterator());
    next(true);
  }

  @Override
  public boolean hasTop() {
    return source.hasTop();
  }

  @Override
  public void next() throws IOException {
    next(false);
  }

  private void next(boolean needSeek) throws IOException {
    if (rowRangeIterator == null)
      throw new IllegalStateException("next() called before seek() b/c rowRangeIterator not set");
    if (!needSeek)
      source.next(); // does nothing if there is no next (i.e. hasTop()==false)
    while (needSeek || (!source.hasTop() && rowRangeIterator.hasNext())) {
      Range range = rowRangeIterator.next();
      range = range.clip(seekRange, true); // clip to the seek range
      if (range == null) // empty intersection - no more ranges by design
        return;
      source.seek(range, columnFamilies, inclusive);
//      sourceIterator = new PeekingIterator1<>(scanner.iterator());
      needSeek = false;
    }
    // either no ranges left and we finished the current scan OR sourceIterator.hasNext()==true
  }

  @Override
  public Key getTopKey() {
    return source.getTopKey();
  }

  @Override
  public Value getTopValue() {
    return source.getTopValue();
  }

  @Override
  public SeekFilterIterator deepCopy(IteratorEnvironment iteratorEnvironment) {
    SeekFilterIterator copy = new SeekFilterIterator(this);
    copy.source = source.deepCopy(iteratorEnvironment);
    return copy;
  }
}
