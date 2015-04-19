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
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Utility functions
 */
public class GraphuloUtil {

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

  /**
   * Convert D4M string representation of rows to Ranges.
   * Last character in the string is an arbitrary separator char
   * that must not appear in the rows. The ':' cannot appear in rows either.
   * See UtilTest for more test cases.
   *
   * @param rowStr Ex: ':,r1,r3,r5,:,r7,r9,:,'
   * @return Ex: (-Inf,r1] [r3,r3) [r5,r7] [r9,+Inf)
   */
  public static Collection<Range> d4mRowToRanges(String rowStr) {
    if (rowStr == null || rowStr.isEmpty())
      return Collections.emptySet();
    // could write my own version that does not do regex, but probably not worth optimizing
    String[] rowStrSplit = rowStr.substring(0, rowStr.length() - 1)
        .split(String.valueOf(rowStr.charAt(rowStr.length() - 1)));
    //if (rowStrSplit.length == 1)
    List<String> rowStrList = Arrays.asList(rowStrSplit);
    PeekingIterator3<String> pi = new PeekingIterator3<>(rowStrList.iterator());
    Set<Range> rngset = new HashSet<>();

    if (pi.peekFirst().equals(":")) { // (-Inf,
      if (pi.peekSecond() == null) {
        return Collections.singleton(new Range()); // (-Inf,+Inf)
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
    return textsToD4mString(texts, ',');
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
    // attach combiner on Ctable
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
