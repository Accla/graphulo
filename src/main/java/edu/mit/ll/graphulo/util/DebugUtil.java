package edu.mit.ll.graphulo.util;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;


/**
 * Helpful for debugging
 */
public class DebugUtil {


  private DebugUtil() {
  }

  public static void printTable(String header, Connector conn, String table) {
    printTable(header, conn, table, null);
  }

  /** @param w Label Width of rows and columns; default 5 */
  public static void printTable(String header, Connector conn, String table, Integer w) {
    if (header != null)
      System.out.println(header);
    Scanner scan;
    try {
      scan = conn.createScanner(table, Authorizations.EMPTY);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    printMapFull(scan.iterator(), w);
    scan.close();
  }


  /** @param w Label Width of rows and columns; default 5 */
  public static void printMapFull(Iterator<Map.Entry<Key, Value>> iter, Integer w) {
    if (w == null)
      w = 5;
    SortedSet<String> columnSet = new TreeSet<>();
    SortedMap<String,SortedMap<String,Value>> rowToColumnMap = new TreeMap<>();

    {
      String curRow = null;
      SortedMap<String, Value> curRowMap = null;

      while (iter.hasNext()) {
        Map.Entry<Key, Value> entry = iter.next();
        Key k = entry.getKey();
        String row = k.getRow().toString();
        String col = k.getColumnQualifier().toString();

        columnSet.add(col);
        if (!row.equals(curRow)) {
          curRow = row;
          curRowMap = new TreeMap<>();
          rowToColumnMap.put(curRow, curRowMap);
        }
        curRowMap.put(col, entry.getValue());
      }
    }

    // print columns
    System.out.printf("%"+w+"s ", "");
    for (String col : columnSet) {
      System.out.printf("%"+w+"s ", col.substring(0, Math.min(w, col.length())));
    }
    System.out.println();

    // print body
    for (Map.Entry<String, SortedMap<String, Value>> rowEntry : rowToColumnMap.entrySet()) {
      String row = rowEntry.getKey();
      SortedMap<String, Value> colMap = rowEntry.getValue();
      System.out.printf("%"+w+"s ", row.substring(0, Math.min(w, row.length())));

      for (String col : columnSet) {
        if (colMap.containsKey(col)) {
          String v = colMap.get(col).toString();
          System.out.printf("%"+w+"s ", v.substring(0, Math.min(w, v.length())));
        } else {
          System.out.printf("%"+w+"s ", "");
        }
      }
      System.out.println();
    }
  }

}
