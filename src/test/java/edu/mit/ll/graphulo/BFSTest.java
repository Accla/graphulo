package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

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
      expectTranspose.putAll(GraphuloUtil.transposeMap(input));
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
    String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, null, -1, tADeg, "", true, 1, 2, Graphulo.PLUS_ITERATOR_BIGDECIMAL);
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
   * Send to client instead of a new table.
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   */
  @Test
  public void testAdjBFSToClient() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tADeg;
    {
      String[] names = getUniqueNames(2);
      tA = names[0];
      tADeg = names[1];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("5".getBytes()));
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes()));
      expect.putAll(input);
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes()));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes()));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes()));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes()));
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

    String v0 = "v0,vBig,";
    Collection<Text> u1expect = GraphuloUtil.d4mRowToTexts("v1,vBig,");

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    String u1actual = graphulo.AdjBFS(tA, v0, 1, null, null, actual, -1, tADeg, "", true, 1, 2, Graphulo.PLUS_ITERATOR_BIGDECIMAL);
    Assert.assertEquals(u1expect, GraphuloUtil.d4mRowToTexts(u1actual));
    Assert.assertEquals(expect, actual);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tADeg);
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
      expectTranspose.putAll(GraphuloUtil.transposeMap(input));
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
      String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, null, -1, tADeg, "deg", false, 1, 2, Graphulo.PLUS_ITERATOR_BIGDECIMAL);
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
      String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, null, -1, tADeg, "d|", true, 1, 2, null);
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

    // now put in range expression for v0
    v0 = "v0,:,v000,";
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
    {
      String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, null, -1, tADeg, "d|", true, 1, 2, null);
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
      expectTranspose.putAll(GraphuloUtil.transposeMap(input));
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
    {
      String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, null, -1, null, null, true, 1, 2, Graphulo.PLUS_ITERATOR_BIGDECIMAL);
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

    // now put in range expression for v0
    v0 = "v0,:,v000,";
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
    {
      String u3actual = graphulo.AdjBFS(tA, v0, 3, tR, tRT, null, -1, null, null, true, 1, 2, null);
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
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  /**
   * Same as above but do all nodes. Effectively copies table and its transpose that passes filter.
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
      expectTranspose.putAll(GraphuloUtil.transposeMap(input));
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
    String u1actual = graphulo.AdjBFS(tA, v0, 1, tR, tRT, null, -1, null, null, true, 1, 2, null);
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

  /**
   * Same as above but no filtering. Effectively copies table and its transpose.
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   */
  @Test
  public void testAdjBFSNoFilter() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
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
      input.put(new Key("vBig", "", "v0"), new Value("9".getBytes()));
      input.put(new Key("vBig", "", "v1"), new Value("9".getBytes()));
      input.put(new Key("vBig", "", "v2"), new Value("9".getBytes()));
      expect.putAll(input);
      expectTranspose.putAll(GraphuloUtil.transposeMap(input));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }

    String v0 = ""; // all nodes
    Collection<Text> u1expect = GraphuloUtil.d4mRowToTexts("v0,v1,v2,vBig,");

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    String u1actual = graphulo.AdjBFS(tA, v0, 1, tR, tRT, null, -1, null, null, true, 1, Integer.MAX_VALUE, null);
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


  /**
   * <pre>
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   * </pre>
   */
  @Test
  public void testEdgeBFS() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tE, tETDeg, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tE = names[0];
      tETDeg = names[1];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("e0", "", "out|v0"), new Value("5".getBytes()));
      input.put(new Key("e0", "", "in|v1"), new Value("5".getBytes()));
      input.put(new Key("e1", "", "out|v1"), new Value("2".getBytes()));
      input.put(new Key("e1", "", "in|v2"), new Value("2".getBytes()));
      input.put(new Key("e2", "", "out|v2"), new Value("4".getBytes()));
      input.put(new Key("e2", "", "in|v0"), new Value("4".getBytes()));
      input.put(new Key("e3", "", "out|v0"), new Value("7".getBytes()));
      input.put(new Key("e3", "", "in|vBig"), new Value("7".getBytes()));
      input.put(new Key("e4", "", "out|v1"), new Value("7".getBytes()));
      input.put(new Key("e4", "", "in|vBig"), new Value("7".getBytes()));
      input.put(new Key("e5", "", "out|v2"), new Value("7".getBytes()));
      input.put(new Key("e5", "", "in|vBig"), new Value("7".getBytes()));
      expect.putAll(input);
      expectTranspose.putAll(GraphuloUtil.transposeMap(input));
      input.put(new Key("e6", "", "out|vBig"), new Value("9".getBytes()));
      input.put(new Key("e6", "", "in|v0"), new Value("9".getBytes()));
      input.put(new Key("e7", "", "out|vBig"), new Value("9".getBytes()));
      input.put(new Key("e7", "", "in|v1"), new Value("9".getBytes()));
      input.put(new Key("e8", "", "out|vBig"), new Value("9".getBytes()));
      input.put(new Key("e8", "", "in|v2"), new Value("9".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("e33"));
      TestUtil.createTestTable(conn, tE, splits, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "2"), new Value("1".getBytes()));
      input.put(new Key("v1", "", "2"), new Value("1".getBytes()));
      input.put(new Key("v2", "", "2"), new Value("1".getBytes()));
      input.put(new Key("vBig", "", "3"), new Value("1".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tETDeg, splits, input);
    }

    String v0 = "v0,";
    Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("v0,vBig,");

    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.EdgeBFS(tE, v0, 3, tR, tRT, "out|,", "in|,", tETDeg, "", true, 1, 2, Graphulo.PLUS_ITERATOR_BIGDECIMAL, 1);
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

    // sanity check out prefixes
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.EdgeBFS(tE, v0, 3, tR, tRT, "out|,afedfafe,", "in|,", tETDeg, "", true, 1, 2, Graphulo.PLUS_ITERATOR_BIGDECIMAL, -1);
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

    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
    v0 = "v0,:,v000,";
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.EdgeBFS(tE, v0, 3, tR, tRT, "out|,", "in|,", tETDeg, "", true, 1, 2, Graphulo.PLUS_ITERATOR_BIGDECIMAL, -1);
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

    conn.tableOperations().delete(tE);
    conn.tableOperations().delete(tETDeg);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  /**
   * <pre>
   *    ->vBig<-
   *   /   ^    \
   *  v    v     v
   * v0--->v1--->v2--v
   *  ^--<------<----/
   * </pre>
   */
  @Test
  public void testEdgeBFS_MultiEdge() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tE, tETDeg, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tE = names[0];
      tETDeg = names[1];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("e0", "", "outA|v0"), new Value("5".getBytes()));
      input.put(new Key("e0", "", "in|v1"), new Value("5".getBytes()));
      input.put(new Key("e1", "", "out|v1"), new Value("2".getBytes()));
      input.put(new Key("e1", "", "HEYHEY|v2"), new Value("2".getBytes()));
      input.put(new Key("e2", "", "outB|v2"), new Value("4".getBytes()));
      input.put(new Key("e2", "", "in|v0"), new Value("4".getBytes()));
      input.put(new Key("e3", "", "out|v0"), new Value("7".getBytes()));
      input.put(new Key("e3", "", "in|vBig"), new Value("7".getBytes()));
      input.put(new Key("e4", "", "v1"), new Value("7".getBytes()));
      input.put(new Key("e4", "", "in|vBig"), new Value("7".getBytes()));
      input.put(new Key("e5", "", "outA|v2"), new Value("7".getBytes()));
      input.put(new Key("e5", "", "in|vBig"), new Value("7".getBytes()));
      expect.putAll(input);
      expectTranspose.putAll(GraphuloUtil.transposeMap(input));
      input.put(new Key("e6", "", "outA|vBig"), new Value("9".getBytes()));
      input.put(new Key("e6", "", "HEYHEY|v0"), new Value("9".getBytes()));
      input.put(new Key("e7", "", "out|vBig"), new Value("9".getBytes()));
      input.put(new Key("e7", "", "in|v1"), new Value("9".getBytes()));
      input.put(new Key("e8", "", "out|vBig"), new Value("9".getBytes()));
      input.put(new Key("e8", "", "in|v2"), new Value("9".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("e33"));
      TestUtil.createTestTable(conn, tE, splits, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "2"), new Value("1".getBytes()));
      input.put(new Key("v1", "", "2"), new Value("1".getBytes()));
      input.put(new Key("v2", "", "2"), new Value("1".getBytes()));
      input.put(new Key("vBig", "", "3"), new Value("1".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tETDeg, splits, input);
    }

    String v0 = "v0,";
    Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("v0,vBig,");

    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.EdgeBFS(tE, v0, 3, tR, tRT, "out|,outB|,outA|,,", "in|,HEYHEY|,", tETDeg, "", true, 1, 2, Graphulo.PLUS_ITERATOR_BIGDECIMAL, -1);
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
    conn.tableOperations().delete(tE);
    conn.tableOperations().delete(tETDeg);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  /**
   * Undirected.
   * <pre>
   *       v9
   *       |
   *    - vBig -
   *   /   |    \
   *  /    |     \
   * v0----v1----v2
   * </pre>
   */
  @Test
  public void testSingleBFS() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tS, tR;
    {
      String[] names = getUniqueNames(2);
      tS = names[0];
      tR = names[1];
    }
    Map<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        degex = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        degin = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0|v1", "", "edge"), new Value("5".getBytes()));
      input.put(new Key("v1|v0", "", "edge"), new Value("5".getBytes()));
      input.put(new Key("v1|v2", "", "edge"), new Value("2".getBytes()));
      input.put(new Key("v2|v1", "", "edge"), new Value("2".getBytes()));
      input.put(new Key("v0|vBig", "", "edge"), new Value("6".getBytes()));
      input.put(new Key("v1|vBig", "", "edge"), new Value("7".getBytes()));
      input.put(new Key("v2|vBig", "", "edge"), new Value("8".getBytes()));
      input.put(new Key("v9|vBig", "", "edge"), new Value("9".getBytes()));
      input.put(new Key("vBig|v0", "", "edge"), new Value("6".getBytes()));
      input.put(new Key("vBig|v1", "", "edge"), new Value("7".getBytes()));
      input.put(new Key("vBig|v2", "", "edge"), new Value("8".getBytes()));
      input.put(new Key("vBig|v9", "", "edge"), new Value("9".getBytes()));
      input.put(new Key("v0", "", "deg"), new Value("2".getBytes()));
      input.put(new Key("v1", "", "deg"), new Value("3".getBytes()));
      input.put(new Key("v2", "", "deg"), new Value("2".getBytes()));
      input.put(new Key("v9", "", "deg"), new Value("1".getBytes()));
      input.put(new Key("vBig", "", "deg"), new Value("4".getBytes()));

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tS, splits, input);

      expect.put(new Key("v0|v1", "", "edge"), new Value("15".getBytes())); //x3
      expect.put(new Key("v1|v0", "", "edge"), new Value("15".getBytes()));
      expect.put(new Key("v1|v2", "", "edge"), new Value("4".getBytes())); //x2
      expect.put(new Key("v2|v1", "", "edge"), new Value("4".getBytes()));
      expect.put(new Key("v0|vBig", "", "edge"), new Value("12".getBytes())); //x2
      expect.put(new Key("v1|vBig", "", "edge"), new Value("7".getBytes()));
      expect.put(new Key("v2|vBig", "", "edge"), new Value("8".getBytes()));
//      expect.put(new Key("v9|vBig", "", "edge"), new Value("9".getBytes()));
      expect.put(new Key("vBig|v0", "", "edge"), new Value("12".getBytes()));
      expect.put(new Key("vBig|v1", "", "edge"), new Value("7".getBytes()));
      expect.put(new Key("vBig|v2", "", "edge"), new Value("8".getBytes()));
//      expect.put(new Key("vBig|v9", "", "edge"), new Value("9".getBytes()));
      degex.put(new Key("v0", "", "deg"), new Value("2".getBytes()));
      degex.put(new Key("v1", "", "deg"), new Value("3".getBytes()));
      degex.put(new Key("v2", "", "deg"), new Value("2".getBytes()));
//      degex.put(new Key("v9", "",   "deg"), new Value("1".getBytes()));
      degin.put(new Key("vBig", "", "deg"), new Value("3".getBytes())); // 3, not 4!!

    }

    IteratorSetting sumSetting = new IteratorSetting(6, SummingCombiner.class);
    LongCombiner.setEncodingType(sumSetting, LongCombiner.Type.STRING);
    Combiner.setColumns(sumSetting, Collections.singletonList(new IteratorSetting.Column("", "edge")));
    // ^^^^^^^^ Important: Combiner only applies to edge column, not to the degree column
    // Want to treat degree as the number of columns, not the sum of weights


    // Below code for experimenting:
//    {
//      // 1 step
//      boolean copyOutDegrees = true, computeInDegrees = false, outputUnion = false;
//      String v0 = "v0,";
//      Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("v1,vBig,");
//      {
//        Graphulo graphulo = new Graphulo(conn, tester.getPassword());
//        String u3actual = graphulo.SingleBFS(tS, "edge", '|', v0, 1, tR,
//            tS, "deg", false, copyOutDegrees, computeInDegrees, 1, 3, sumSetting, outputUnion, true);
//
//
//        BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
//        scanner.setRanges(Collections.singleton(new Range()));
//        for (Map.Entry<Key, Value> entry : scanner) {
//          actual.put(entry.getKey(), entry.getValue());
//          System.out.println(entry.getKey().toStringNoTime()+" -> "+entry.getValue());
//        }
//        scanner.close();
//  //      TestUtil.printExpectActual(expect, actual);
//        Assert.assertEquals(expect, actual);
//        Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));
//      }
//    }
//    if (true) return;
//    conn.tableOperations().delete(tR);

    boolean copyOutDegrees = false, computeInDegrees = false, outputUnion = false;
    String v0 = "v0,";
    Collection<Text> u3expect = GraphuloUtil.d4mRowToTexts("v1,vBig,");
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.SingleBFS(tS, "edge", '|', v0, 3, tR,
          tS, "deg", false, copyOutDegrees, computeInDegrees, 1, 3, sumSetting, outputUnion);


      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
//      TestUtil.printExpectActual(expect, actual);
      Assert.assertEquals(expect, actual);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));
    }

    conn.tableOperations().delete(tR);
    copyOutDegrees = false; computeInDegrees = false; outputUnion = true;
    u3expect = GraphuloUtil.d4mRowToTexts("v0,v1,v2,vBig,");
    v0 = "v0,:,v000,";
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.SingleBFS(tS, "edge", '|', v0, 3, tR,
          tS, "deg", false, copyOutDegrees, computeInDegrees, 1, 3, sumSetting, outputUnion);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);
    }

    conn.tableOperations().delete(tR);
    copyOutDegrees = true; computeInDegrees = false; outputUnion = true;
    v0 = "v0,";
    expect.putAll(degex);
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.SingleBFS(tS, "edge", '|', v0, 3, tR,
          tS, "deg", false, copyOutDegrees, computeInDegrees, 1, 3, sumSetting, outputUnion);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      //TestUtil.printExpectActual(expect, actual);
      Assert.assertEquals(expect, actual);
    }

    conn.tableOperations().delete(tR);
    copyOutDegrees = true; computeInDegrees = false; outputUnion = false;
    u3expect = GraphuloUtil.d4mRowToTexts("v1,vBig,");
    v0 = "v0,:,v000,";
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.SingleBFS(tS, "edge", '|', v0, 3, tR,
          tS, "deg", false, copyOutDegrees, computeInDegrees, 1, 3, sumSetting, outputUnion);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);
    }

    conn.tableOperations().delete(tR);
    copyOutDegrees = true; computeInDegrees = true; outputUnion = false;
    u3expect = GraphuloUtil.d4mRowToTexts("v1,vBig,");
    v0 = "v0,";
    expect.putAll(degin);
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String u3actual = graphulo.SingleBFS(tS, "edge", '|', v0, 3, tR,
          tS, "deg", false, copyOutDegrees, computeInDegrees, 1, 3, sumSetting, outputUnion);
      Assert.assertEquals(u3expect, GraphuloUtil.d4mRowToTexts(u3actual));

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      TestUtil.printExpectActual(expect, actual);
      Assert.assertEquals(expect, actual);
    }


    conn.tableOperations().delete(tS);
    conn.tableOperations().delete(tR);
  }



  @Test
  public void testGenerateDegreeTable() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tE, tET, tEDegOutIn, tEDegOut, tEDegIn, tEDegEdge, tEDegWeightEdge;
    {
      String[] names = getUniqueNames(7);
      tE = names[0];
      tET = names[1];
      tEDegOutIn = names[2];
      tEDegOut = names[3];
      tEDegIn = names[4];
      tEDegEdge = names[5];
      tEDegWeightEdge = names[6];
    }
    Map<Key,Value>
//        expectOut = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
//        actualOut = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
//        expectIn = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
//        actualIn = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
//        expectOutIn = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
//        actualOutIn = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectEdgeDeg = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualEdgeDeg = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectEdgeDegWeight = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualEdgeDegWeight = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("e0", "", "out|v0"), new Value("5".getBytes()));
      input.put(new Key("e0", "", "in|v1"), new Value("5".getBytes()));
      input.put(new Key("e1", "", "out|v1"), new Value("2".getBytes()));
      input.put(new Key("e1", "", "in|v2"), new Value("2".getBytes()));
      input.put(new Key("e2", "", "out|v2"), new Value("4".getBytes()));
      input.put(new Key("e2", "", "in|v0"), new Value("4".getBytes()));
      input.put(new Key("e3", "", "out|v0"), new Value("7".getBytes()));
      input.put(new Key("e3", "", "in|vBig"), new Value("7".getBytes()));
      input.put(new Key("e4", "", "out|v1"), new Value("7".getBytes()));
      input.put(new Key("e4", "", "in|vBig"), new Value("7".getBytes()));
      input.put(new Key("e5", "", "out|v2"), new Value("7".getBytes()));
      input.put(new Key("e5", "", "in|vBig"), new Value("7".getBytes()));
      input.put(new Key("e6", "", "out|vBig"), new Value("9".getBytes()));
      input.put(new Key("e6", "", "in|v0"), new Value("9".getBytes()));
      input.put(new Key("e7", "", "out|vBig"), new Value("9".getBytes()));
      input.put(new Key("e7", "", "in|v1"), new Value("9".getBytes()));
      input.put(new Key("e8", "", "out|vBig"), new Value("9".getBytes()));
      input.put(new Key("e8", "", "in|v2"), new Value("9".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("e33"));
      TestUtil.createTestTable(conn, tE, splits, input);
//      splits.clear();
//      splits.add(new Text("out|v11"));
//      TestUtil.createTestTable(conn, tET, splits, TestUtil.transposeMap(input));
    }
    {
//      expectOut.put(new Key("v0", "",   ""), new Value("2".getBytes()));
//      expectOut.put(new Key("v1", "",   ""), new Value("2".getBytes()));
//      expectOut.put(new Key("v2", "",   ""), new Value("2".getBytes()));
//      expectOut.put(new Key("vBig", "", ""), new Value("3".getBytes()));
//
//      expectIn.put(new Key("v0", "",   ""), new Value("2".getBytes()));
//      expectIn.put(new Key("v1", "",   ""), new Value("2".getBytes()));
//      expectIn.put(new Key("v2", "",   ""), new Value("2".getBytes()));
//      expectIn.put(new Key("vBig", "", ""), new Value("3".getBytes()));
//
//      expectOutIn.put(new Key("v0", "",   ""), new Value("4".getBytes()));
//      expectOutIn.put(new Key("v1", "",   ""), new Value("4".getBytes()));
//      expectOutIn.put(new Key("v2", "",   ""), new Value("4".getBytes()));
//      expectOutIn.put(new Key("vBig", "", ""), new Value("6".getBytes()));

      expectEdgeDeg.put(new Key("e0", "", ""), new Value("2".getBytes()));
      expectEdgeDeg.put(new Key("e1", "", ""), new Value("2".getBytes()));
      expectEdgeDeg.put(new Key("e2", "", ""), new Value("2".getBytes()));
      expectEdgeDeg.put(new Key("e3", "", ""), new Value("2".getBytes()));
      expectEdgeDeg.put(new Key("e4", "", ""), new Value("2".getBytes()));
      expectEdgeDeg.put(new Key("e5", "", ""), new Value("2".getBytes()));
      expectEdgeDeg.put(new Key("e6", "", ""), new Value("2".getBytes()));
      expectEdgeDeg.put(new Key("e7", "", ""), new Value("2".getBytes()));
      expectEdgeDeg.put(new Key("e8", "", ""), new Value("2".getBytes()));

      expectEdgeDegWeight.put(new Key("e0", "", ""), new Value("10".getBytes()));
      expectEdgeDegWeight.put(new Key("e1", "", ""), new Value("4".getBytes()));
      expectEdgeDegWeight.put(new Key("e2", "", ""), new Value("8".getBytes()));
      expectEdgeDegWeight.put(new Key("e3", "", ""), new Value("14".getBytes()));
      expectEdgeDegWeight.put(new Key("e4", "", ""), new Value("14".getBytes()));
      expectEdgeDegWeight.put(new Key("e5", "", ""), new Value("14".getBytes()));
      expectEdgeDegWeight.put(new Key("e6", "", ""), new Value("18".getBytes()));
      expectEdgeDegWeight.put(new Key("e7", "", ""), new Value("18".getBytes()));
      expectEdgeDegWeight.put(new Key("e8", "", ""), new Value("18".getBytes()));
    }

    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long numRows = graphulo.generateDegreeTable(tE, tEDegEdge, true);

      BatchScanner scanner = conn.createBatchScanner(tEDegEdge, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualEdgeDeg.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectEdgeDeg, actualEdgeDeg);
      Assert.assertEquals(9, numRows);
    }
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long numRows = graphulo.generateDegreeTable(tE, tEDegWeightEdge, false);

      BatchScanner scanner = conn.createBatchScanner(tEDegWeightEdge, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualEdgeDegWeight.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectEdgeDegWeight, actualEdgeDegWeight);
      Assert.assertEquals(9, numRows);
    }

    conn.tableOperations().delete(tE);
//    conn.tableOperations().delete(tET);
    conn.tableOperations().delete(tEDegEdge);
    conn.tableOperations().delete(tEDegWeightEdge);
//    conn.tableOperations().delete(tEDegOut);
  }


}
