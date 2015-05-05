package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.mult.BigDecimalMultiply;
import edu.mit.ll.graphulo.mult.LongMultiply;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

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

    final String tC, tAT, tB;
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
      input = TestUtil.tranposeMap(input);
      TestUtil.createTestTable(conn, tAT, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value("3".getBytes()));
      input.put(new Key("B1", "", "C3"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C1"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C2"), new Value("3".getBytes()));
      input = TestUtil.tranposeMap(input);
      TestUtil.createTestTable(conn, tB, null, input);
    }
    Map<Key,Value> expect = new TreeMap<Key, Value>(TestUtil.COMPARE_KEY_TO_COLQ);
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes()));
    expect.put(new Key("A1", "", "B2"), new Value("21".getBytes()));
    expect.put(new Key("A2", "", "B2"), new Value("12".getBytes()));
    expect = Collections.unmodifiableMap(expect);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    graphulo.TableMult(tAT, tB, tC,
        LongMultiply.class, new IteratorSetting(1, "sum", BigDecimalCombiner.BigDecimalSummingCombiner.class),
        null, null, null, 1, true);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expect, actual);

    conn.tableOperations().delete(tAT);
    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
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
      input = TestUtil.tranposeMap(input);
      TestUtil.createTestTable(conn, tAT, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value("3".getBytes()));
      input.put(new Key("B1", "", "C3"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C1"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C2"), new Value("3".getBytes()));
      input = TestUtil.tranposeMap(input);
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("C15"));
      TestUtil.createTestTable(conn, tB, splits, input);
    }

    TestUtil.createTestTable(conn, tC);

    Map<Key,Value> expect = new HashMap<Key, Value>();
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes()));
    expect.put(new Key("A1", "", "B2"), new Value("6".getBytes()));
    expect = Collections.unmodifiableMap(expect);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    graphulo.TableMult(tAT, tB, tC,
        BigDecimalMultiply.class, new IteratorSetting(1, "sum", BigDecimalCombiner.BigDecimalSummingCombiner.class),
        GraphuloUtil.d4mRowToRanges("C2,:,"), null, null, 1, false);

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
      input = TestUtil.tranposeMap(input);
      TestUtil.createTestTable(conn, tAT, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value("3".getBytes()));
      input.put(new Key("B1", "", "C3"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C1"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C2"), new Value("3".getBytes()));
      input = TestUtil.tranposeMap(input);
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("C15"));
      TestUtil.createTestTable(conn, tB, splits, input);
    }
    TestUtil.createTestTable(conn, tC);

    Map<Key,Value> expect = new HashMap<Key, Value>();
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes()));
    expect = Collections.unmodifiableMap(expect);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    graphulo.TableMult(tAT, tB, tC,
        BigDecimalMultiply.class, new IteratorSetting(1, "sum", BigDecimalCombiner.BigDecimalSummingCombiner.class),
        GraphuloUtil.d4mRowToRanges("C2,:,"),
        "A1,", "B1,", 1, false);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    System.out.println(expect.equals(actual));
    System.out.println(actual.equals(expect));
    Assert.assertEquals(expect, actual);

    conn.tableOperations().delete(tAT);
    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
  }

}
