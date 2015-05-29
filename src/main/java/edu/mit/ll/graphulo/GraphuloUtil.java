package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Utility functions
 */
public class GraphuloUtil {
  private static final Logger log = LogManager.getLogger(GraphuloUtil.class);

  /**
   * Split options on period characters.
   * "" holds entries without a period.
   *
   * @return Map Prefix -> (Map entryWithoutPrefix -> value)
   */
  public static Map<String, Map<String, String>> splitMapPrefix(Map<String, String> options) {
    Map<String, Map<String, String>> prefixMap = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();
      int periodIdx = key.indexOf(".");
      if (periodIdx == 0)
        throw new IllegalArgumentException("not allowed to have option that starts with period: " + entry);
      String prefix = periodIdx == -1
          ? "" : key.substring(0, periodIdx);
      String afterPrefix = periodIdx == -1
          ? key : key.substring(periodIdx + 1); // ok if empty

      Map<String, String> mapInside = prefixMap.get(prefix);
      if (mapInside == null) {
        mapInside = new HashMap<>();
      }
      mapInside.put(afterPrefix, entry.getValue());
      prefixMap.put(prefix, mapInside);
    }
    return prefixMap;
  }

  public static Map<String, String> preprendPrefixToKey(String prefix, Map<String, String> options) {
    Map<String, String> res = new HashMap<>(options.size());
    for (Map.Entry<String, String> entry : options.entrySet()) {
      res.put(prefix + entry.getKey(), entry.getValue());
    }
    return res;
  }

  private static final SortedSet<Range> SETINFRNG;
  static {
    SortedSet<Range> tmp = new TreeSet<>();
    tmp.add(new Range());
    SETINFRNG = Collections.unmodifiableSortedSet(tmp);
  }

  /**
   * Convert D4M string representation of rows to Ranges.
   * Last character in the string is an arbitrary separator char
   * that must not appear in the rows. The ':' cannot appear in rows either.
   * See UtilTest for more test cases.
   * Does not merge overlapping ranges.
   *
   * @param rowStr Ex: ':,r1,r3,r5,:,r7,r9,:,'
   * @return Ex: (-Inf,r1] [r3,r3) [r5,r7] [r9,+Inf)
   */
  public static SortedSet<Range> d4mRowToRanges(String rowStr) {
    if (rowStr == null || rowStr.isEmpty())
      return new TreeSet<>();
    // could write my own version that does not do regex, but probably not worth optimizing
    String[] rowStrSplit = rowStr.substring(0, rowStr.length() - 1)
        .split(String.valueOf(rowStr.charAt(rowStr.length() - 1)));
    //if (rowStrSplit.length == 1)
    List<String> rowStrList = Arrays.asList(rowStrSplit);
    PeekingIterator3<String> pi = new PeekingIterator3<>(rowStrList.iterator());
    SortedSet<Range> rngset = new TreeSet<>();

    if (pi.peekFirst().equals(":")) { // (-Inf,
      if (pi.peekSecond() == null) {
        return SETINFRNG; // (-Inf,+Inf)
      } else {
        if (pi.peekSecond().equals(":") || (pi.peekThird() != null && pi.peekThird().equals(":")))
          throw new IllegalArgumentException("Bad D4M rowStr: " + rowStr);
        rngset.add(new Range(null, false, pi.peekSecond(), true)); // (-Inf,2]
        pi.next();
        pi.next();
      }
    }

    while (pi.hasNext()) {
      if (pi.peekSecond() == null) { // last singleton row [1,1~)
        Key key = new Key(pi.peekFirst());
        rngset.add(new Range(key, true, key.followingKey(PartialKey.ROW), false));
        return rngset;
      } else if (pi.peekSecond().equals(":")) {
        if (pi.peekThird() == null) { // [1,+Inf)
          rngset.add(new Range(pi.peekFirst(), true, null, false));
          return rngset;
        } else { // [1,3)
          if (pi.peekThird().equals(":"))
            throw new IllegalArgumentException("Bad D4M rowStr: " + rowStr);
          rngset.add(new Range(pi.peekFirst(), true, pi.peekThird(), true));
          pi.next();
          pi.next();
          pi.next();
        }
      } else { // [1,1~)
        Key key = new Key(pi.peekFirst());
        rngset.add(new Range(key, true, key.followingKey(PartialKey.ROW), false));
        pi.next();
      }
    }
    return rngset;
  }

  public static String rangesToD4MString(Collection<Range> ranges) {
    return rangesToD4MString(ranges, '\t');
  }

  public static String rangesToD4MString(Collection<Range> ranges, char sep) {
    if (ranges == null || ranges.isEmpty())
      return "";
    ranges = Range.mergeOverlapping(ranges);
    StringBuilder sb = new StringBuilder();
    String infEnd = null;
    for (Range range : ranges) {
      if (range.isInfiniteStartKey() && range.isInfiniteStopKey())
        return sb.append(':').append(sep).toString();
      else if (range.isInfiniteStartKey()) {
        String endRow = normalizeEndRow(range); assert endRow != null;
        sb.insert(0, ":" + sep+endRow+sep);
      } else if (range.isInfiniteStopKey()) {
        infEnd = normalizeStartRow(range); assert infEnd != null;
      } else {
        String startRow = normalizeStartRow(range),
            endRow = normalizeEndRow(range);
        assert startRow != null && endRow != null;
        if (startRow.equals(endRow))
          sb.append(startRow).append(sep);
        else
          sb.append(startRow).append(sep).append(':').append(sep).append(endRow).append(sep);
      }
    }
    if (infEnd != null)
      sb.append(infEnd).append(sep).append(':').append(sep);
    return sb.toString();
  }

  private static String normalizeStartRow(Range range) {
    Key startKey = range.getStartKey();
    if (startKey == null)
      return null;
    String startRow = new String(startKey.getRowData().getBackingArray());
    if (!range.isStartKeyInclusive())
      return startRow+'\0';
    else
      return startRow;
  }

  private static String normalizeEndRow(Range range) {
    Key endKey = range.getEndKey();
    if (endKey == null)
      return null;
    String endRow = new String(endKey.getRowData().getBackingArray());
    if (!range.isEndKeyInclusive())
      return prevRow(endRow);
    else
      return endRow;
  }

  private static String prevRow(String row) {
    if (row.charAt(row.length()-1) == '\0')
      return row.substring(0, row.length() - 1);
    else
      return row.substring(0,row.length()-1)+ (char)((int)row.charAt(row.length()-1)-1);
  }

  /**
   * Convert D4M string representation of individual rows/columns to Text objects.
   * No ':' character allowed!
   * Last character in the string is an arbitrary separator char
   * that must not appear in the rows. The ':' cannot appear in rows either.
   * See UtilTest for more test cases.
   *
   * @param rowStr Ex: 'a,b,c,d,'
   * @return A Text object for each one.
   */
  public static Collection<Text> d4mRowToTexts(String rowStr) {
    if (rowStr == null || rowStr.isEmpty())
      return Collections.emptySet();
    // could write my own version that does not do regex, but probably not worth optimizing
    String[] rowStrSplit = rowStr.substring(0, rowStr.length() - 1)
        .split(String.valueOf(rowStr.charAt(rowStr.length() - 1)));
    Collection<Text> ts = new HashSet<>(rowStrSplit.length);
    for (String row : rowStrSplit) {
      if (row.equals(":"))
        throw new IllegalArgumentException("rowStr cannot contain ranges: " + rowStr);
      ts.add(new Text(row));
    }
    return ts;
  }

  public static String textsToD4mString(Collection<Text> texts) {
    return textsToD4mString(texts, '\t');
  }

  public static String textsToD4mString(Collection<Text> texts, char sep) {
    if (texts == null)
      return "";
    StringBuilder sb = new StringBuilder();
    for (Text text : texts) {
      sb.append(text).append(sep);
    }
    return sb.toString();
  }

  public static Collection<Range> textsToRanges(Collection<Text> texts) {
    Collection<Range> ranges = new HashSet<>();
    for (Text text : texts) {
      ranges.add(new Range(text));
    }
    return ranges;
  }

  /**
   * Add BigDecimalSummingCombiner on as many scopes as possible (scan, minc, majc).
   * Call it "plus".
   */
  public static void addCombiner(TableOperations tops, String table, Logger log) {
    // TODO P2: Assign priority and name dynamically, checking for conflicts.
    Map<String, String> optSum = new HashMap<>();
    optSum.put("all", "true");
    IteratorSetting iSum = new IteratorSetting(19,"plus",BigDecimalCombiner.BigDecimalSummingCombiner.class, optSum);

    // checking if iterator already exists. Not checking for conflicts.
    try {
      IteratorSetting existing;
      EnumSet<IteratorUtil.IteratorScope> enumSet = EnumSet.noneOf(IteratorUtil.IteratorScope.class);
      for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
        existing = tops.getIteratorSetting(table, "plus", scope);
        if (existing == null)
          enumSet.add(scope);
      }
      tops.attachIterator(table, iSum, enumSet);
    } catch (AccumuloSecurityException | AccumuloException e) {
      log.error("error trying to add BigDecimalSummingCombiner to " + table, e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("no table: "+table, e);
      throw new RuntimeException(e);
    }
  }


  /**
   * Add the given Iterator to a table on scan, minc, majc scopes.
   * If already present on a scope, does not re-add the iterator to that scope.
   * Call it "plus".
   */
  public static void applyIteratorSoft(IteratorSetting itset, TableOperations tops, String table) {
    // checking if iterator already exists. Not checking for conflicts.
    try {
      IteratorSetting existing;
      EnumSet<IteratorUtil.IteratorScope> enumSet = EnumSet.noneOf(IteratorUtil.IteratorScope.class);
      for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
        existing = tops.getIteratorSetting(table, "plus", scope);
        if (existing == null)
          enumSet.add(scope);
      }
      tops.attachIterator(table, itset, enumSet);
    } catch (AccumuloSecurityException | AccumuloException e) {
      log.error("error trying to add "+itset+" to " + table, e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("no table: "+table, e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Remove "plus" combiner from every scope it is configured on.
   */
  public static void removeCombiner(TableOperations tops, String table, Logger log) {
    // remove on the scopes the iterator is set on
    try {
      IteratorSetting existing;
      EnumSet<IteratorUtil.IteratorScope> enumSet = EnumSet.allOf(IteratorUtil.IteratorScope.class);
      for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
        existing = tops.getIteratorSetting(table, "plus", scope);
        if (existing == null)
          enumSet.remove(scope);
      }
      tops.removeIterator(table, "plus", enumSet);
    } catch (AccumuloSecurityException | AccumuloException e) {
      log.error("error trying to remove \"plus\" Combiner from " + table, e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("no table: "+table, e);
      throw new RuntimeException(e);
    }
  }

  private static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * Create a copy of key with all fields except the ones specified cleared.
   * @param key The key to copy
   * @param pk What fields to retain from the key
   * @return A new Key object pointing to new copies of fields specified by pk; other fields are empty/default.
   */
  public static Key keyCopy(Key key, PartialKey pk) {
    if (key == null || pk == null)
      return null;
    switch (pk) {
      case ROW:
        return new Key(key.getRowData().getBackingArray(), EMPTY_BYTES, EMPTY_BYTES, EMPTY_BYTES, Long.MAX_VALUE, false, true);
      case ROW_COLFAM:
        return new Key(key.getRowData().getBackingArray(), key.getColumnFamilyData().getBackingArray(), EMPTY_BYTES, EMPTY_BYTES, Long.MAX_VALUE, false, true);
      case ROW_COLFAM_COLQUAL:
        return new Key(key.getRowData().getBackingArray(), key.getColumnFamilyData().getBackingArray(), key.getColumnQualifierData().getBackingArray(), EMPTY_BYTES, Long.MAX_VALUE, false, true);
      case ROW_COLFAM_COLQUAL_COLVIS:
        return new Key(key.getRowData().getBackingArray(), key.getColumnFamilyData().getBackingArray(), key.getColumnQualifierData().getBackingArray(), key.getColumnVisibilityData().getBackingArray(), Long.MAX_VALUE, false, true);
      case ROW_COLFAM_COLQUAL_COLVIS_TIME:
        return new Key(key.getRowData().getBackingArray(), key.getColumnFamilyData().getBackingArray(), key.getColumnQualifierData().getBackingArray(), key.getColumnVisibilityData().getBackingArray(), key.getTimestamp(), false, true);
      case ROW_COLFAM_COLQUAL_COLVIS_TIME_DEL:
        return new Key(key.getRowData().getBackingArray(), key.getColumnFamilyData().getBackingArray(), key.getColumnQualifierData().getBackingArray(), key.getColumnVisibilityData().getBackingArray(), key.getTimestamp(), key.isDeleted(), true);
      default:
        throw new AssertionError();
    }
  }

//  public static void addIteratorDynamically(TableOperations tops, String tableName,
//                                     IteratorSetting setting, EnumSet<IteratorUtil.IteratorScope> scopes,
//                                     boolean beforeVers)
//      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
//    int maxPriority = Integer.MAX_VALUE;
//    if (beforeVers) {
//      IteratorSetting versSetting = tops.getIteratorSetting(tableName, "vers", scopes.iterator().next());
//      if (versSetting != null)
//        maxPriority = versSetting.getPriority();
//    }
//    tops.checkIteratorConflicts(tableName, setting, scopes);
//  }


}
