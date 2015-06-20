package edu.mit.ll.graphulo.util;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.skvi.D4mColumnRangeFilter;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.ColumnQualifierFilter;
import org.apache.accumulo.core.iterators.user.ColumnSliceFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Utility functions
 */
public class GraphuloUtil {
  private static final Logger log = LogManager.getLogger(GraphuloUtil.class);

  public static final char DEFAULT_SEP_D4M_STRING = '\t';
  private static final Text EMPTY_TEXT = new Text();


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
    return d4mRowToRanges(rowStr, false);
  }

  /**
   * @see #d4mRowToRanges(String)
   * @param singletonsArePrefix If true, then singleton entries in the D4M string are
   *                            made into prefix ranges instead of single row ranges.
   */
  public static SortedSet<Range> d4mRowToRanges(String rowStr, boolean singletonsArePrefix) {
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
        if (singletonsArePrefix)
          rngset.add(Range.prefix(pi.peekFirst()));
        else
          rngset.add(Range.exact(pi.peekFirst()));
        return rngset;
      } else if (pi.peekSecond().equals(":")) {
        if (pi.peekThird() == null) { // [1,+Inf)
          rngset.add(new Range(pi.peekFirst(), true, null, false));
          return rngset;
        } else { // [1,3]
          if (pi.peekThird().equals(":"))
            throw new IllegalArgumentException("Bad D4M rowStr: " + rowStr);
          rngset.add(new Range(pi.peekFirst(), true, pi.peekThird(), true));
          pi.next();
          pi.next();
          pi.next();
        }
      } else { // [1,1~)
        if (singletonsArePrefix)
          rngset.add(Range.prefix(pi.peekFirst()));
        else
          rngset.add(Range.exact(pi.peekFirst()));
        pi.next();
      }
    }
    return rngset;
  }

  public static String rangesToD4MString(Collection<Range> ranges) {
    return rangesToD4MString(ranges, DEFAULT_SEP_D4M_STRING);
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

  public static final char LAST_ONE_BYTE_CHAR = Character.toChars(Byte.MAX_VALUE)[0];

  private static String prevRow(String row) {
    if (row.charAt(row.length()-1) == '\0')
      return row.substring(0, row.length() - 1);
    else
      return row.substring(0,row.length()-1)
          + (char)((int)row.charAt(row.length()-1)-1)
          + LAST_ONE_BYTE_CHAR;
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
    return textsToD4mString(texts, DEFAULT_SEP_D4M_STRING);
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
        existing = tops.getIteratorSetting(table, itset.getName(), scope);
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
        throw new AssertionError("unknown pk: "+pk);
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

//  * @param permitFetchColumns True allows use of {@link ScannerBase#fetchColumn}. False forces use of {@link ColumnQualifierFilter} explicitly.
//  *                           Set to false if filtering columns upstream would create large difficulty.

  /**
   * Apply an appropriate column filter based on the input string.
   * Four modes of operation:
   1. Null or blank ("") `colFilter`: do nothing.
   2. No ranges `colFilter`: use scanner.fetchColumn() which invokes an Accumulo system ColumnQualifierFilter.
   3. Singleton range `colFilter`: use Accumulo user ColumnSliceFilter.
   4. Multi-range `colFilter`: use Graphulo D4mColumnRangeFilter.
   * @param colFilter column filter string
   * @param scanner to call fetchColumn() on, for case #2
   * @param priority to use for scan iterator setting, for case #3 and #4
   */
  public static void applyGeneralColumnFilter(String colFilter, ScannerBase scanner, int priority) {
//    System.err.println("colFilter: "+colFilter);
    if (colFilter != null && !colFilter.isEmpty()) {
      int pos1 = colFilter.indexOf(':');
      if (pos1 == -1) { // no ranges - collection of singleton texts
        for (Text text : GraphuloUtil.d4mRowToTexts(colFilter)) {
          scanner.fetchColumn(GraphuloUtil.EMPTY_TEXT, text);
        }
      } else {
        SortedSet<Range> ranges = GraphuloUtil.d4mRowToRanges(colFilter);
        assert ranges.size() > 0;
        IteratorSetting s;
        if (ranges.size() == 1) { // single range - use ColumnSliceFilter
          Range r = ranges.first();
          s = new IteratorSetting(priority, ColumnSliceFilter.class);
//          System.err.println("start: "+(r.isInfiniteStartKey() ? null : r.getStartKey().getRow().toString())
//              +"end: "+(r.isInfiniteStopKey() ? null : r.getEndKey().getRow().toString()));
          ColumnSliceFilter.setSlice(s, r.isInfiniteStartKey() ? null : r.getStartKey().getRow().toString(),
              true, r.isInfiniteStopKey() ? null : r.getEndKey().getRow().toString(), true);
//          System.err.println("!ok "+GraphuloUtil.d4mRowToRanges(colFilter));
        } else { // multiple ranges
//          System.err.println("ok "+GraphuloUtil.d4mRowToRanges(colFilter));
          Map<String,String> map = new HashMap<>();
          map.put(D4mColumnRangeFilter.COLRANGES,colFilter);
          s = new IteratorSetting(priority, D4mColumnRangeFilter.class, map);
        }
        scanner.addScanIterator(s);
      }
    }
  }

  /**
   * For use within an iterator stack.
   * Apply an appropriate column filter based on the input string.
   * Four modes of operation:
   1. Null or blank ("") `colFilter`: do nothing.
   2. No ranges `colFilter`: use Accumulo system ColumnQualifierFilter.
   3. Singleton range `colFilter`: use Accumulo user ColumnSliceFilter.
   4. Multi-range `colFilter`: use Graphulo D4mColumnRangeFilter.
   * @param colFilter column filter string
   * @param skvi Parent / source iterator
   * @return SKVI with appropriate filter iterators placed in front of it.
   */
  public static SortedKeyValueIterator<Key,Value> applyGeneralColumnFilter(
      String colFilter, SortedKeyValueIterator<Key,Value> skvi, IteratorEnvironment env) throws IOException {
    if (colFilter == null || colFilter.isEmpty())
      return skvi;

    int pos1 = colFilter.indexOf(':');
    if (pos1 == -1) { // no ranges - collection of singleton texts
      Set<Column> colset = new HashSet<>();
      for (Text text : GraphuloUtil.d4mRowToTexts(colFilter)) {
        colset.add(new Column(EMPTY_BYTES, text.getBytes(), EMPTY_BYTES));
      }
      return new ColumnQualifierFilter(skvi, colset);

    } else {
      SortedSet<Range> ranges = GraphuloUtil.d4mRowToRanges(colFilter);
      assert ranges.size() > 0;

      if (ranges.size() == 1) { // single range - use ColumnSliceFilter
        Range r = ranges.first();
        Map<String,String> map = new HashMap<>();

        String start = r.isInfiniteStartKey() ? null : r.getStartKey().getRow().toString(),
            end = r.isInfiniteStopKey() ? null : r.getEndKey().getRow().toString();
        boolean startInclusive = true, endInclusive = true;

        if (start != null)
          map.put(ColumnSliceFilter.START_BOUND, start);
        if (end != null)
          map.put(ColumnSliceFilter.END_BOUND, end);
        map.put(ColumnSliceFilter.START_INCLUSIVE, String.valueOf(startInclusive));
        map.put(ColumnSliceFilter.END_INCLUSIVE, String.valueOf(endInclusive));

        SortedKeyValueIterator<Key,Value> filter = new ColumnSliceFilter();
        filter.init(skvi, map, env);
        return filter;

      } else { // multiple ranges
        Map<String,String> map = new HashMap<>();
        map.put(D4mColumnRangeFilter.COLRANGES,colFilter);

        SortedKeyValueIterator<Key,Value> filter = new D4mColumnRangeFilter();
        filter.init(skvi, map, env);
        return filter;
      }
    }
  }

  public static <E> E subclassNewInstance(String classname, Class<E> parentClass) {
    Class<?> c;
    try {
      c = Class.forName(classname);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Can't find class: " + classname, e);
    }
    Class<? extends E> cm;
    try {
      cm = c.asSubclass(parentClass);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(classname+" is not a subclass of "+parentClass.getName(), e);
    }
    try {
      return cm.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalArgumentException("can't instantiate new instance of " + cm.getName(), e);
    }
  }


  /** If str begins with prefix, return a String containing the characters after the prefix. Otherwise return null. */
  public static String stringAfter(byte[] prefix, byte[] str) {
    return 0 == WritableComparator.compareBytes(str, 0, prefix.length, prefix, 0, prefix.length)
        ? new String(str, prefix.length, str.length - prefix.length, UTF_8)
        : null;
  }

  /** Add prefix and/or suffix to every part of a D4M string.
   * prependStartPrefix("pre|","X","a,b,:,v,:,") ==>
   *  "pre|aX,pre|bX,:,pre|vX,:,"
   * */
  public static String padD4mString(String prefix, String suffix, String str) {
    if (prefix == null)
      prefix = "";
    if (suffix == null)
      suffix = "";
    if (prefix.isEmpty() && suffix.isEmpty())
      return str;
    char sep = str.charAt(str.length() - 1);
    StringBuilder sb = new StringBuilder();
    String[] split = str.split(String.valueOf(sep));
    for (String part : split) {
      if (part.equals(":"))
        sb.append(part).append(sep);
      else
        sb.append(prefix).append(part).append(suffix).append(sep);
    }
    return sb.toString();
  }

  /**
   * Makes each input term into a prefix range.
   * <pre>
   *  "v1,v5," => "v1|,:,v1},v5|,:,v5},"
   *  "v1,:,v3,v5," => "v1,:,v3,v5|,:,v5},"
   * </pre>
   */
  public static String singletonsAsPrefix(String str) {
    Preconditions.checkNotNull(str);
    Preconditions.checkArgument(!str.isEmpty());
//    Preconditions.checkArgument(str.indexOf(':') != -1, "Cannot have the ':' character: "+str);
    char sep = str.charAt(str.length() - 1);

    if (str.indexOf(':') == -1) {
      StringBuilder sb = new StringBuilder();
      for (String vktext : str.split(String.valueOf(sep))) {
        sb.append(vktext).append(sep)
            .append(':').append(sep)
            .append(prevRow(Range.followingPrefix(new Text(vktext)).toString()))
            .append(sep);
      }
      return sb.toString();
    }

//    Collection<Range> origRngs = d4mRowToRanges(str);
//    for (Range rng : origRngs) {
//      // if a singleton row, then make into a prefix row
//
//    }

//    String[] strSplit = str.substring(0, str.length() - 1)
//        .split(String.valueOf(sep));
//    List<String> strList = Arrays.asList(strSplit);
//    PeekingIterator3<String> pi = new PeekingIterator3<>(strList.iterator());
//    SortedSet<Range> rngset = new TreeSet<>();
//
//    if (pi.peekFirst().equals(":")) { // (-Inf,
//      if (pi.peekSecond() == null) {
//        return str; // (-Inf,+Inf)
//      } else {
//        if (pi.peekSecond().equals(":") || (pi.peekThird() != null && pi.peekThird().equals(":")))
//          throw new IllegalArgumentException("Bad D4M rowStr: " + str);
////        sb.append(':').append(sep).append(pi.peekSecond()).append(sep); // (-Inf,2]
//        rngset.add(new Range(null, false, pi.peekSecond(), true)); // (-Inf,2]
//        pi.next();
//        pi.next();
//      }
//    }
//
//    while (pi.hasNext()) {
//      if (pi.peekSecond() == null) { // last singleton row [1,1~)
////        sb.append(pi.peekFirst()).append(sep)
////            .append(':').append(sep)
////            .append(Range.followingPrefix(new Text(pi.peekFirst())).toString()).append(sep);
//        rngset.add(Range.prefix(pi.peekFirst()));
//
//      } else if (pi.peekSecond().equals(":")) {
//        if (pi.peekThird() == null) { // [1,+Inf)
////          sb.append(pi.peekFirst()).append(sep).append(':').append(sep);
//          rngset.add(new Range(pi.peekFirst(), true, null, false));
//
//        } else { String s = GraphuloUtil.singletonsAsPrefix(vktexts, sep);// [1,3]
//          if (pi.peekThird().equals(":"))
//            throw new IllegalArgumentException("Bad D4M rowStr: " + str);
////          sb.append(pi.peekFirst()).append(sep)
////              .append(':').append(sep)
////              .append(pi.peekThird()).append(sep);
//          rngset.add(new Range(pi.peekFirst(), true, pi.peekThird(), true));
//          pi.next();
//          pi.next();
//          pi.next();
//        }
//      } else { // [1,1~)
////        sb.append(pi.peekFirst()).append(sep)
////            .append(':').append(sep)
////            .append(Range.followingPrefix(new Text(pi.peekFirst())).toString()).append(sep);
//        rngset.add(Range.prefix(pi.peekFirst()));
//        pi.next();
//      }
//    }

    Collection<Range> prefixRngs = d4mRowToRanges(str, true);
//    log.info(prefixRngs);
    return rangesToD4MString(prefixRngs, sep);
  }

  /**
   * Makes each input term into a prefix range.
   * "v1,v5," => "v1,:,v1\255,v5,:,v5\255,"
   */
  public static String singletonsAsPrefix(Collection<Text> vktexts, char sep) {
    StringBuilder sb = new StringBuilder();
    for (Text vktext : vktexts) {
      sb.append(vktext.toString()).append(sep)
          .append(':').append(sep)
          .append(prevRow(Range.followingPrefix(new Text(vktext)).toString()))
          .append(sep);
    }
    return sb.toString();
  }


}
