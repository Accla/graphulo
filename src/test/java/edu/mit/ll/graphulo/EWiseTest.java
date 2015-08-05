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
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 *
 */
public class EWiseTest extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(EWiseTest.class);

  /**
   * <pre>
   *      C1 C2        C1 C2 C3          C1  C2
   * A1 [ 5  2 ] .*A1 [   3  3  ] = A1 [     6  ]
   * A2 [ 4    ]   A2 [3  3     ]   A2 [ 12     ]
   * </pre>
   */
  @Test
  public void testEWiseX() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
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
      Map<Key,Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes()));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes()));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes()));
      TestUtil.createTestTable(conn, tAT, null, input);
    }
    {
      Map<Key,Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C2"), new Value("3".getBytes()));
      input.put(new Key("A1", "", "C3"), new Value("3".getBytes()));
      input.put(new Key("A2", "", "C1"), new Value("3".getBytes()));
      input.put(new Key("A2", "", "C2"), new Value("3".getBytes()));
      TestUtil.createTestTable(conn, tB, null, input);
    }
    SortedMap<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    expect.put(new Key("A1", "", "C2"), new Value("6".getBytes()));
    expect.put(new Key("A2", "", "C1"), new Value("12".getBytes()));
    SortedMap<Key,Value> expectT = GraphuloUtil.transposeMap(expect);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    graphulo.SpEWiseX(tAT, tB, tC, tCT, -1, MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG), null, null, null, null, 1);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    {
      SortedMap<Key,Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key,Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);
    }

    scanner = conn.createScanner(tCT, Authorizations.EMPTY);
    {
      SortedMap<Key,Value> actualT = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key,Value> entry : scanner) {
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

  @Test
  public void testEWiseX_SCC() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tC, tA, tB;
    {
      String[] names = getUniqueNames(4);
      tA = names[0];
      tB = names[1];
      tC = names[2];
    }

    Map<Key,Value> expect = new TreeMap<Key,Value>(TestUtil.COMPARE_KEY_TO_COLQ);
    {
      Map<Key,Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v0"), new Value("1".getBytes()));
      input.put(new Key("v0", "", "v1"), new Value("1".getBytes()));
      input.put(new Key("v0", "", "v2"), new Value("1".getBytes()));
      input.put(new Key("v1", "", "v0"), new Value("1".getBytes()));
      input.put(new Key("v1", "", "v1"), new Value("1".getBytes()));
      input.put(new Key("v1", "", "v2"), new Value("1".getBytes()));
      input.put(new Key("v2", "", "v0"), new Value("1".getBytes()));
      input.put(new Key("v2", "", "v1"), new Value("1".getBytes()));
      input.put(new Key("v2", "", "v2"), new Value("1".getBytes()));
      input.put(new Key("vBig", "", "v0"), new Value("1".getBytes()));
      input.put(new Key("vBig", "", "v1"), new Value("1".getBytes()));
      input.put(new Key("vBig", "", "v2"), new Value("1".getBytes()));

      Map<Key,Value> input2 = new HashMap<>();
      input2.put(new Key("v0", "", "v0"), new Value("1".getBytes()));
      input2.put(new Key("v0", "", "v1"), new Value("1".getBytes()));
      input2.put(new Key("v0", "", "v2"), new Value("1".getBytes()));
      input2.put(new Key("v1", "", "v0"), new Value("1".getBytes()));
      input2.put(new Key("v1", "", "v1"), new Value("1".getBytes()));
      input2.put(new Key("v1", "", "v2"), new Value("1".getBytes()));
      input2.put(new Key("v2", "", "v0"), new Value("1".getBytes()));
      input2.put(new Key("v2", "", "v1"), new Value("1".getBytes()));
      input2.put(new Key("v2", "", "v2"), new Value("1".getBytes()));
      input2.put(new Key("v0", "", "vBig"), new Value("1".getBytes()));
      input2.put(new Key("v1", "", "vBig"), new Value("1".getBytes()));
      input2.put(new Key("v2", "", "vBig"), new Value("1".getBytes()));

      Map<Key,Value> e = new HashMap<>();
      e.put(new Key("v0", "", "v1"), new Value("1".getBytes()));
      e.put(new Key("v0", "", "v2"), new Value("1".getBytes()));
      e.put(new Key("v0", "", "v0"), new Value("1".getBytes()));

      e.put(new Key("v1", "", "v2"), new Value("1".getBytes()));
      e.put(new Key("v1", "", "v0"), new Value("1".getBytes()));
      e.put(new Key("v1", "", "v1"), new Value("1".getBytes()));

      e.put(new Key("v2", "", "v0"), new Value("1".getBytes()));
      e.put(new Key("v2", "", "v1"), new Value("1".getBytes()));
      e.put(new Key("v2", "", "v2"), new Value("1".getBytes()));

      expect.putAll(e);
      TestUtil.createTestTable(conn, tA, null, input);
      TestUtil.createTestTable(conn, tB, null, input2);
    }

    Graphulo g = new Graphulo(conn, tester.getPassword());
    g.SpEWiseX(tA, tB, tC, null, -1, MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG), null, null, null, null, -1);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    {
      SortedMap<Key,Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key,Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);
    }
  }

  /**
   * <pre>
   *      C1 C2        C1 C2 C3          C1 C2 C3
   * A1 [ 5  2 ] .+A1 [   3  3  ] = A1 [ 5  5  3 ]
   * A2 [ 4    ]   A2 [3  3     ]   A2 [ 7  3    ]
   * </pre>
   */
  @Test
  public void testEWiseSum() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();

    final String tC, tA, tB, tCT;
    {
      String[] names = getUniqueNames(4);
      tA = names[0];
      tB = names[1];
      tC = names[2];
      tCT = names[3];
    }
    {
      Map<Key,Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C1"), new Value("5".getBytes()));
      input.put(new Key("A1", "", "C2"), new Value("2".getBytes()));
      input.put(new Key("A2", "", "C1"), new Value("4".getBytes()));
      TestUtil.createTestTable(conn, tA, null, input);
    }
    {
      Map<Key,Value> input = new HashMap<>();
      input.put(new Key("A1", "", "C2"), new Value("3".getBytes()));
      input.put(new Key("A1", "", "C3"), new Value("3".getBytes()));
      input.put(new Key("A2", "", "C1"), new Value("3".getBytes()));
      input.put(new Key("A2", "", "C2"), new Value("3".getBytes()));
      TestUtil.createTestTable(conn, tB, null, input);
    }
    SortedMap<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    expect.put(new Key("A1", "", "C1"), new Value("5".getBytes()));
    expect.put(new Key("A1", "", "C2"), new Value("5".getBytes()));
    expect.put(new Key("A1", "", "C3"), new Value("3".getBytes()));
    expect.put(new Key("A2", "", "C1"), new Value("7".getBytes()));
    expect.put(new Key("A2", "", "C2"), new Value("3".getBytes()));
    SortedMap<Key,Value> expectT = GraphuloUtil.transposeMap(expect);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    graphulo.SpEWiseSum(tA, tB, tC, tCT, -1, MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.PLUS, MathTwoScalar.ScalarType.LONG), null, null, null, null, 1);

    Scanner scanner = conn.createScanner(tC, Authorizations.EMPTY);
    {
      SortedMap<Key,Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key,Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);
    }

    scanner = conn.createScanner(tCT, Authorizations.EMPTY);
    {
      SortedMap<Key,Value> actualT = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ
      for (Map.Entry<Key,Value> entry : scanner) {
        actualT.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectT, actualT);
    }

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tB);
    conn.tableOperations().delete(tC);
    conn.tableOperations().delete(tCT);
  }

}
