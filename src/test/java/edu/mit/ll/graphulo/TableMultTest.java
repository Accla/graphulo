package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Test TableMult in {@link Graphulo}.
 */
public class TableMultTest extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(TableMultTest.class);

  /**
   * <pre>
   *      C1 C2        C1 C2 C3          B1  B2
   * A1 [ 5  2 ] * B1 [   3  3  ] = A1 [ 6   15+6 ]
   * A2 [ 4    ]   B2 [3  3     ]   A2 [     12   ]
   * </pre>
   */
  @Test
//  @Ignore("New version only works with BatchWriter. KnownBug: ACCUMULO-3645")
  public void test1() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tC, tAT, tB, tCT;
    {
      String[] names = getUniqueNames(4);
      tAT = names[0];
      tB = names[1];
      tC = names[2];
      tCT = names[3];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes()));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes()));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes()));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tAT, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value("3".getBytes()));
      input.put(new Key("B1", "", "C3"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C1"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C2"), new Value("3".getBytes()));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tB, null, input);
    }
    SortedMap<Key, Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes()));
    expect.put(new Key("A1", "", "B2"), new Value("21".getBytes()));
    expect.put(new Key("A2", "", "B2"), new Value("12".getBytes()));
    SortedMap<Key, Value> expectT = GraphuloUtil.transposeMap(expect);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numpp = graphulo.TableMult(tAT, tB, tC, tCT, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.BIGDECIMAL), Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        null, null, null, false, false, 1);

    Assert.assertEquals(4, numpp);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);
    }

    scanner = conn.createScanner(tCT, Authorizations.EMPTY);
    {
      SortedMap<Key, Value> actualT = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key, Value> entry : scanner) {
        actualT.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectT, actualT);
    }

    conn.tableOperations().delete(tAT);
    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
    conn.tableOperations().delete(tCT);
  }

  /**
   * <pre>
   *      C1 C2        C1 C2 C3          B1  B2
   * A1 [ x  2 ] * B1 [   3  3  ] = A1 [ 6   6 ]
   * A2 [ x    ]   B2 [x  3     ]   A2 [       ]
   * </pre>
   */
  @Test
  public void testRowFilter() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tAT, tB, tC;
    {
      String[] names = getUniqueNames(3);
      tAT = names[0];
      tB = names[1];
      tC = names[2];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes()));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes()));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes()));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tAT, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value("3".getBytes()));
      input.put(new Key("B1", "", "C3"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C1"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C2"), new Value("3".getBytes()));
      input = GraphuloUtil.transposeMap(input);
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("C15"));
      TestUtil.createTestTable(conn, tB, splits, input);
    }

    TestUtil.createTestTable(conn, tC);

    Map<Key, Value> expect = new HashMap<>();
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes()));
    expect.put(new Key("A1", "", "B2"), new Value("6".getBytes()));
    expect = Collections.unmodifiableMap(expect);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numpp = graphulo.TableMult(tAT, tB, tC, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG), Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        GraphuloUtil.d4mRowToRanges("C2,:,"), null, null, false, false, 1);

    Assert.assertEquals(2, numpp);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);

    conn.tableOperations().delete(tAT);
    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
  }

  /**
   * <pre>
   *      C1 C2        C1 C2 C3          B1  B2
   * A1 [ x  2 ] * B1 [   3  3  ] = A1 [ 6     ]
   * A2 [ x    ]   B2 [x  xx    ]   A2 [       ]
   * </pre>
   */
  @Test
  public void testRowFilterColFilterATB() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tAT, tB, tC, tCT;
    {
      String[] names = getUniqueNames(4);
      tAT = names[0];
      tB = names[1];
      tC = names[2];
      tCT = names[3];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes()));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes()));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes()));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tAT, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value("3".getBytes()));
      input.put(new Key("B1", "", "C3"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C1"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C2"), new Value("3".getBytes()));
      input = GraphuloUtil.transposeMap(input);
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("C15"));
      TestUtil.createTestTable(conn, tB, splits, input);
    }
    TestUtil.createTestTable(conn, tC);

    Map<Key, Value> expect = new HashMap<>();
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes()));

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numpp = graphulo.TableMult(tAT, tB, tC, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG), Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        GraphuloUtil.d4mRowToRanges("C2,:,"),
        "A1,", "B1,", false, false, 1);

    Assert.assertEquals(1, numpp);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);
    scanner.close();

    // now check more advanced column filter, write to transpose
    expect = GraphuloUtil.transposeMap(expect);
    graphulo.TableMult(tAT, tB, null, tCT, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG), Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        GraphuloUtil.d4mRowToRanges("C2,:,"),
        "A1,:,A15,", "B1,:,B15,F,", false, false, 1);
    scanner = conn.createScanner(tCT, Authorizations.EMPTY);
    actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);
    scanner.close();

    conn.tableOperations().delete(tAT);
    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
    conn.tableOperations().delete(tCT);
  }


  /**
   * Adds in A*A at the same time as A*B.
   * <pre>
   *      C1 C2        C1 C2 C3          B1  B2
   * A1 [ x  2 ] * B1 [   3  3  ] = A1 [ 6     ]
   * A2 [ x    ]   B2 [x  xx    ]   A2 [       ]
   * </pre>
   */
  @Test
  public void testAlsoDoAA() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tAT, tB, tC, tCT;
    {
      String[] names = getUniqueNames(4);
      tAT = names[0];
      tB = names[1];
      tC = names[2];
      tCT = names[3];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes()));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes()));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes()));
      input = GraphuloUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tAT, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value("3".getBytes()));
      input.put(new Key("B1", "", "C3"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C1"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C2"), new Value("3".getBytes()));
      input = GraphuloUtil.transposeMap(input);
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("C15"));
      TestUtil.createTestTable(conn, tB, splits, input);
    }
    TestUtil.createTestTable(conn, tC);

    Map<Key, Value> expect = new HashMap<>();
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes()));
    expect.put(new Key("A1", "", "A1"), new Value("4".getBytes()));

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numpp = graphulo.TableMult(tAT, tB, tC, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG), Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        GraphuloUtil.d4mRowToRanges("C2,:,"),
        "A1,", "B1,", true, false, 1);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);
    Assert.assertEquals(2, numpp);
    scanner.close();

    // now check more advanced column filter, write to transpose
    expect = GraphuloUtil.transposeMap(expect);
    graphulo.TableMult(tAT, tB, null, tCT, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG), Graphulo.PLUS_ITERATOR_BIGDECIMAL,
        GraphuloUtil.d4mRowToRanges("C2,:,"),
        "A1,:,A15,", "B1,:,B15,F,", true, false, 1);
    scanner = conn.createScanner(tCT, Authorizations.EMPTY);
    actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);
    scanner.close();

    conn.tableOperations().delete(tAT);
    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
    conn.tableOperations().delete(tCT);
  }

}
