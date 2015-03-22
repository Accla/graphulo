package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.mult.BigDecimalMultiply;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
import org.apache.accumulo.core.security.Authorizations;
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

//  /** Uses dummy writes to fix result  */
//  public static final boolean BUGPATH_ACCUMULO_3645=true;

  /**
   * <pre>
   *      C1 C2        C1 C2 C3          B1  B2
   * A1 [ 5  2 ] * B1 [   3  3  ] = A1 [ 6   15+6 ]
   * A2 [ 4    ]   B2 [3  3     ]   A2 [     12   ]
   * </pre>
   */
  @Test
  @Ignore("KnownBug: ACCUMULO-3645")
  public void test1() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tP, tA, tBT;
    {
      String[] names = getUniqueNames(3);
      tP = names[0];
      tA = names[1];
      tBT = names[2];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes()));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes()));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes()));
      TestUtil.createTestTable(conn, tA, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value("3".getBytes()));
      input.put(new Key("B1", "", "C3"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C1"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C2"), new Value("3".getBytes()));
      TestUtil.createTestTable(conn, tBT, null, input);
    }
    Map<Key,Value> expect = new HashMap<Key, Value>();
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes()));
    expect.put(new Key("A1", "", "B2"), new Value("21".getBytes()));
    expect.put(new Key("A2", "", "B2"), new Value("12".getBytes()));
    expect = Collections.unmodifiableMap(expect);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    graphulo.TableMult(tP, tA, tBT,
        BigDecimalMultiply.class, BigDecimalCombiner.BigDecimalSummingCombiner.class,
        null, null, null, null, true);

    Scanner scanner = conn.createScanner(tP, Authorizations.EMPTY);
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(expect, actual);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tBT);
    conn.tableOperations().delete(tP);
  }

  /**
   * Uses dummy write to get around ACCUMULO-3645.
   * <pre>
   *      C1 C2        C1 C2 C3          B1  B2
   * A1 [ 5  2 ] * B1 [   3  3  ] = A1 [ 6   15+6 ]
   * A2 [ 4    ]   B2 [3  3     ]   A2 [     12   ]
   * </pre>
   */
  @Test
  public void test2() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tP, tA, tBT;
    {
      String[] names = getUniqueNames(3);
      tP = names[0];
      tA = names[1];
      tBT = names[2];
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes()));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes()));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes()));
      TestUtil.createTestTable(conn, tA, null, input);
    }
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("B1", "", "C2"), new Value("3".getBytes()));
      input.put(new Key("B1", "", "C3"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C1"), new Value("3".getBytes()));
      input.put(new Key("B2", "", "C2"), new Value("3".getBytes()));
      TestUtil.createTestTable(conn, tBT, null, input);
    }

    // ACCUMULO-3645 bugfix
    Key dummykey = new Key();
    dummykey.setDeleted(true);
    Map<Key,Value> dummyMap = Collections.singletonMap(dummykey, new Value("1".getBytes()));
    TestUtil.createTestTable(conn, tP, null, dummyMap);

    Map<Key,Value> expect = new HashMap<Key, Value>();
    expect.put(new Key("A1", "", "B1"), new Value("6".getBytes()));
    expect.put(new Key("A1", "", "B2"), new Value("21".getBytes()));
    expect.put(new Key("A2", "", "B2"), new Value("12".getBytes()));
        //.putAll(dummyMap)
    expect = Collections.unmodifiableMap(expect);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    graphulo.TableMult(tP, tA, tBT,
        BigDecimalMultiply.class, BigDecimalCombiner.BigDecimalSummingCombiner.class,
        null, null, null, null, true);

    Scanner scanner = conn.createScanner(tP, Authorizations.EMPTY);
    Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    System.out.println(expect.equals(actual));
    System.out.println(actual.equals(expect));
    Assert.assertEquals(expect, actual);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tBT);
    conn.tableOperations().delete(tP);
  }

}
