package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;

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
   *   that must not appear in the rows. The ':' cannot appear in rows either.
   * See UtilTest for more test cases.
   * @param rowStr Ex: ':,r1,r3,r5,:,r7,r9,:,'
   * @return Ex: (-Inf,r1] [r3,r3) [r5,r7] [r9,+Inf)
   */
  public static Collection<Range> d4mRowToRanges(String rowStr) {
    if (rowStr == null || rowStr.isEmpty())
      return Collections.emptySet();
    // could write my own version that does not do regex, but probably not worth optimizing
    String[] rowStrSplit = rowStr.substring(0,rowStr.length()-1)
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
          throw new IllegalArgumentException("Bad D4M rowStr: "+rowStr);
        rngset.add(new Range(null,false,pi.peekSecond(),true)); // (-Inf,2]
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
            throw new IllegalArgumentException("Bad D4M rowStr: "+rowStr);
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
   *   that must not appear in the rows. The ':' cannot appear in rows either.
   * See UtilTest for more test cases.
   * @param rowStr Ex: 'a,b,c,d,'
   * @return A Text object for each one.
   */
  public static Collection<Text> d4mRowToTexts(String rowStr) {
    if (rowStr == null || rowStr.isEmpty())
      return Collections.emptySet();
    // could write my own version that does not do regex, but probably not worth optimizing
    String[] rowStrSplit = rowStr.substring(0,rowStr.length()-1)
        .split(String.valueOf(rowStr.charAt(rowStr.length() - 1)));
    Collection<Text> ts = new HashSet<>(rowStrSplit.length);
    for (String row : rowStrSplit) {
      if (row.equals(":"))
        throw new IllegalArgumentException("rowStr cannot contain ranges: "+rowStr);
      ts.add(new Text(row));
    }
    return ts;
  }

}
