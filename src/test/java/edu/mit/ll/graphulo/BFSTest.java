package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class BFSTest extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(BFSTest.class);

  /**
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   */
  @Test
  public void testAdjBFS() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tADeg, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tA = names[0];
      tADeg = names[1];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
      actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
      expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
      actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("5".getBytes()));
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes()));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes()));
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes()));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes()));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes()));
      expect.putAll(input);
      expectTranspose.putAll(TestUtil.transposeMap(input));
      input.put(new Key("vBig", "", "v0"), new Value("9".getBytes()));
      input.put(new Key("vBig", "", "v1"), new Value("9".getBytes()));
      input.put(new Key("vBig", "", "v2"), new Value("9".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "2"), new Value("1".getBytes()));
      input.put(new Key("v1", "", "2"), new Value("1".getBytes()));
      input.put(new Key("v2", "", "2"), new Value("1".getBytes()));
      input.put(new Key("vBig", "", "3"), new Value("1".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tADeg, splits, input);
    }

    String v0 = "v0,";
    Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("v0,vBig,");

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, tADeg, "", true, 1, 2, Graphulo.DEFAULT_PLUS_ITERATOR, true);
    Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expect, actual);

    scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actualTranspose.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expectTranspose, actualTranspose);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tADeg);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  /**
   * Vary degree tables.
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   */
  @Test
  public void testAdjBFSVaryDegreeTable() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tADeg, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tA = names[0];
      tADeg = names[1];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("5".getBytes()));
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes()));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes()));
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes()));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes()));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes()));
      expect.putAll(input);
      expectTranspose.putAll(TestUtil.transposeMap(input));
      input.put(new Key("vBig", "", "v0"), new Value("9".getBytes()));
      input.put(new Key("vBig", "", "v1"), new Value("9".getBytes()));
      input.put(new Key("vBig", "", "v2"), new Value("9".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v2", "", "deg"), new Value("2".getBytes()));
      input.put(new Key("v0", "", "deg"), new Value("2".getBytes()));
      input.put(new Key("v1", "", "deg"), new Value("2".getBytes()));
      input.put(new Key("vBig", "", "deg"), new Value("3".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tADeg, splits, input);
    }

    String v0 = "v0,";
    Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("v0,vBig,");

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    {
      String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, tADeg, "deg", false, 1, 2, Graphulo.DEFAULT_PLUS_ITERATOR, true);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);

      scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualTranspose.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectTranspose, actualTranspose);
    }


    // now put degree in column with prefix
    conn.tableOperations().delete(tADeg);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "d|2"), new Value("1".getBytes()));
      input.put(new Key("v1", "", "d|2"), new Value("1".getBytes()));
      input.put(new Key("v2", "", "d|2"), new Value("1".getBytes()));
      input.put(new Key("vBig", "", "d|3"), new Value("1".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tADeg, splits, input);
    }
    {
      String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, tADeg, "d|", true, 1, 2, null, true);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);

      scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualTranspose.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectTranspose, actualTranspose);
    }


    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tADeg);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  /**
   * Same as above but no degree table.
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   */
  @Test
  public void testAdjBFSNoDegTable() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tA = names[0];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
            actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
            expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
            actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("5".getBytes()));
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes()));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes()));
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes()));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes()));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes()));
      expect.putAll(input);
      expectTranspose.putAll(TestUtil.transposeMap(input));
      input.put(new Key("vBig", "", "v0"), new Value("9".getBytes()));
      input.put(new Key("vBig", "", "v1"), new Value("9".getBytes()));
      input.put(new Key("vBig", "", "v2"), new Value("9".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }

    String v0 = "v0,";
    Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("v0,vBig,");

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, null, "", true, 1, 2, Graphulo.DEFAULT_PLUS_ITERATOR, true);
    Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expect, actual);

    scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actualTranspose.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expectTranspose, actualTranspose);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  /**
   * Same as above but do all nodes. Effectively copies table and its transpose.
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   */
  @Test
  public void testAdjBFSAll() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tA = names[0];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("5".getBytes()));
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes()));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes()));
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes()));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes()));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes()));
      expect.putAll(input);
      expectTranspose.putAll(TestUtil.transposeMap(input));
      input.put(new Key("vBig", "", "v0"), new Value("9".getBytes()));
      input.put(new Key("vBig", "", "v1"), new Value("9".getBytes()));
      input.put(new Key("vBig", "", "v2"), new Value("9".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }

    String v0 = ""; // all nodes
    Collection<Text> u1expect = GraphuloUtil.d4mRowToTexts("v0,v1,v2,vBig,");

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    String u1actual = graphulo.AdjBFS(tA, v0, 1, tR, tRT, null, "", true, 1, 2, null, true);
    Assert.assertEquals(u1expect, GraphuloUtil.d4mRowToTexts(u1actual));

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expect, actual);

    scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actualTranspose.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expectTranspose, actualTranspose);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }


}
