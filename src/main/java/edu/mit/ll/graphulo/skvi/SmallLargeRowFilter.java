package edu.mit.ll.graphulo.skvi;


import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

/**
 * Used for on-the-fly degree filtering with min and max degree.
 * Extension of {@link org.apache.accumulo.core.iterators.user.LargeRowFilter}.
 */
public class SmallLargeRowFilter implements SortedKeyValueIterator<Key, Value>, OptionDescriber {

  public static final Value SUPPRESS_ROW_VALUE = new Value("SUPPRESS_ROW".getBytes(StandardCharsets.UTF_8));

  private static final ByteSequence EMPTY = new ArrayByteSequence(new byte[]{});

  /* key into hash map, value refers to the row suppression limit (maxColumns) */
  private static final String MAX_COLUMNS = "max_columns";
  private static final String MIN_COLUMNS = "min_columns";

  private SortedKeyValueIterator<Key, Value> source;

  // a cache of keys
  private ArrayList<Key> keys = new ArrayList<Key>();
  private ArrayList<Value> values = new ArrayList<Value>();

  private int currentPosition;

  private int maxColumns;
  private int minColumns;

  private boolean propogateSuppression = false;

  private Range range;
  private Collection<ByteSequence> columnFamilies;
  private boolean inclusive;
  private boolean dropEmptyColFams;

  private boolean isSuppressionMarker(Key key, Value val) {
    return key.getColumnFamilyData().length() == 0 && key.getColumnQualifierData().length() == 0 && key.getColumnVisibilityData().length() == 0
        && val.equals(SUPPRESS_ROW_VALUE);
  }

  private void reseek(Key key) throws IOException {
    if (range.afterEndKey(key)) {
      range = new Range(range.getEndKey(), true, range.getEndKey(), range.isEndKeyInclusive());
      source.seek(range, columnFamilies, inclusive);
    } else {
      range = new Range(key, true, range.getEndKey(), range.isEndKeyInclusive());
      source.seek(range, columnFamilies, inclusive);
    }
  }

  private void consumeRow(ByteSequence row) throws IOException {
    // try reading a few and if still not to next row, then seek
    int count = 0;

    while (source.hasTop() && source.getTopKey().getRowData().equals(row)) {
      source.next();
      count++;
      if (count >= 10) {
        Key nextRowStart = new Key(new Text(row.toArray())).followingKey(PartialKey.ROW);
        reseek(nextRowStart);
        count = 0;
      }
    }
  }

  private void addKeyValue(Key k, Value v) {
    if (dropEmptyColFams && k.getColumnFamilyData().equals(EMPTY)) {
      return;
    }
    keys.add(new Key(k));
    values.add(new Value(v));
  }

  private void bufferNextRow() throws IOException {

    keys.clear();
    values.clear();
    currentPosition = 0;

    while (source.hasTop() && keys.size() == 0) {

      addKeyValue(source.getTopKey(), source.getTopValue());

      if (isSuppressionMarker(source.getTopKey(), source.getTopValue())) {

        consumeRow(source.getTopKey().getRowData());

      } else {

        ByteSequence currentRow = keys.get(0).getRowData();
        source.next();

        while (source.hasTop() && source.getTopKey().getRowData().equals(currentRow)) {

          addKeyValue(source.getTopKey(), source.getTopValue());

          if (keys.size() > maxColumns) {
            keys.clear();
            values.clear();

            // when the row is too big, emit a suppression marker
            addKeyValue(new Key(new Text(currentRow.toArray())), SUPPRESS_ROW_VALUE);
            consumeRow(currentRow);
          } else {
            source.next();
          }
        }

        // check min columns
        if (keys.size() < minColumns) {
          keys.clear();
          values.clear();

          // when the row is too small, emit a suppression marker
          addKeyValue(new Key(new Text(currentRow.toArray())), SUPPRESS_ROW_VALUE);
          consumeRow(currentRow);
        }

      }

    }
  }

  private void readNextRow() throws IOException {

    bufferNextRow();

    while (!propogateSuppression && currentPosition < keys.size() && isSuppressionMarker(keys.get(0), values.get(0))) {
      bufferNextRow();
    }
  }

  private SmallLargeRowFilter(SortedKeyValueIterator<Key, Value> source, boolean propogateSuppression, int minColumns, int maxColumns) {
    this.source = source;
    this.propogateSuppression = propogateSuppression;
    this.maxColumns = maxColumns;
  }

  public SmallLargeRowFilter() {
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
    this.maxColumns = options.containsKey(MAX_COLUMNS) ? Integer.parseInt(options.get(MAX_COLUMNS)) : Integer.MAX_VALUE;
    this.minColumns = options.containsKey(MIN_COLUMNS) ? Integer.parseInt(options.get(MIN_COLUMNS)) : 1;
    this.propogateSuppression = env.getIteratorScope() != IteratorUtil.IteratorScope.scan;
  }

  @Override
  public boolean hasTop() {
    return currentPosition < keys.size();
  }

  @Override
  public void next() throws IOException {

    if (currentPosition >= keys.size()) {
      throw new IllegalStateException("Called next() when hasTop() is false");
    }

    currentPosition++;

    if (currentPosition == keys.size()) {
      readNextRow();
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {

    if (inclusive && !columnFamilies.contains(EMPTY)) {
      columnFamilies = new HashSet<ByteSequence>(columnFamilies);
      columnFamilies.add(EMPTY);
      dropEmptyColFams = true;
    } else if (!inclusive && columnFamilies.contains(EMPTY)) {
      columnFamilies = new HashSet<ByteSequence>(columnFamilies);
      columnFamilies.remove(EMPTY);
      dropEmptyColFams = true;
    } else {
      dropEmptyColFams = false;
    }

    this.range = range;
    this.columnFamilies = columnFamilies;
    this.inclusive = inclusive;

    if (range.getStartKey() != null) {
      // seek to beginning of row to see if there is a suppression marker
      Range newRange = new Range(new Key(range.getStartKey().getRow()), true, range.getEndKey(), range.isEndKeyInclusive());
      source.seek(newRange, columnFamilies, inclusive);

      readNextRow();

      // it is possible that all or some of the data read for the current
      // row is before the start of the range
      while (currentPosition < keys.size() && range.beforeStartKey(keys.get(currentPosition)))
        currentPosition++;

      if (currentPosition == keys.size())
        readNextRow();

    } else {
      source.seek(range, columnFamilies, inclusive);
      readNextRow();
    }

  }

  @Override
  public Key getTopKey() {
    return keys.get(currentPosition);
  }

  @Override
  public Value getTopValue() {
    return values.get(currentPosition);
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    return new SmallLargeRowFilter(source.deepCopy(env), propogateSuppression, minColumns, maxColumns);
  }

  @Override
  public IteratorOptions describeOptions() {
    String description = "This iterator suppresses rows that exceed a specified number of columns. Once\n"
        + "a row exceeds the threshold, a marker is emitted and the row is always\n" + "suppressed by this iterator after that point in time.\n"
        + " This iterator works in a similar way to the RowDeletingIterator. See its\n" + " javadoc about locality groups.\n";
    return new IteratorOptions(this.getClass().getSimpleName(), description, Collections.singletonMap(MAX_COLUMNS, "Number Of Columns To Begin Suppression"),
        null);
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    if (options == null || options.size() < 1) {
      throw new IllegalArgumentException("Bad # of options, must supply: " + MAX_COLUMNS + " or "+MIN_COLUMNS);
    }

    if (options.containsKey(MAX_COLUMNS))
      try {
        maxColumns = Integer.parseInt(options.get(MAX_COLUMNS));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("bad integer " + MAX_COLUMNS + ":" + options.get(MAX_COLUMNS));
      }
    else maxColumns = Integer.MAX_VALUE;

    if (options.containsKey(MIN_COLUMNS))
      try {
        minColumns = Integer.parseInt(options.get(MIN_COLUMNS));
        if (minColumns < 1)
          throw new IllegalArgumentException("bad integer " + MIN_COLUMNS + ":" + options.get(MIN_COLUMNS));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("bad integer " + MIN_COLUMNS + ":" + options.get(MIN_COLUMNS));
      }
    else minColumns = 1;

    if (maxColumns < minColumns)
      throw new IllegalArgumentException(MAX_COLUMNS+"="+maxColumns+" should be > than "+MIN_COLUMNS+"="+minColumns);

    return true;
  }

  public static IteratorSetting iteratorSetting(int priority, int minColumns, int maxColumns) {
    IteratorSetting itset = new IteratorSetting(priority, SmallLargeRowFilter.class);
    if (minColumns > 1)
      itset.addOption(MIN_COLUMNS, Integer.toString(minColumns));
    if (maxColumns < Integer.MAX_VALUE)
      itset.addOption(MAX_COLUMNS, Integer.toString(maxColumns));
    return itset;
  }

  /**
   * A convenience method for setting the maximum number of columns to keep.
   *
   * @param is         IteratorSetting object to configure.
   * @param maxColumns maximum #columns
   */
  public static void setMaxColumns(IteratorSetting is, int maxColumns) {
    is.addOption(MAX_COLUMNS, Integer.toString(maxColumns));
  }

  /**
   * A convenience method for setting the minimum number of columns to keep.
   *
   * @param is         IteratorSetting object to configure.
   * @param minColumns minimum #columns
   */
  public static void setMinColumns(IteratorSetting is, int minColumns) {
    is.addOption(MIN_COLUMNS, Integer.toString(minColumns));
  }
}
