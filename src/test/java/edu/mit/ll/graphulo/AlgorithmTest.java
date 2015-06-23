package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Test kTruss, Jaccard and other algorithms.
 */
public class AlgorithmTest extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(AlgorithmTest.class);


  @Test
  public void testkTrussAdj() throws TableNotFoundException {
    Connector conn = tester.getConnector();
    final String tA, tR;
    {
      String[] names = getUniqueNames(2);
      tA = names[0];
      tR = names[1];
    }

    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v1", "", "v2"), new Value("1".getBytes()));
      input.put(new Key("v1", "", "v3"), new Value("1".getBytes()));
      input.put(new Key("v1", "", "v4"), new Value("1".getBytes()));
      input.put(new Key("v2", "", "v3"), new Value("1".getBytes()));
      input.put(new Key("v3", "", "v4"), new Value("1".getBytes()));
      input.putAll(TestUtil.transposeMap(input));
      expect.putAll(input);
      input.put(new Key("v2", "", "v5"), new Value("1".getBytes()));
      input.put(new Key("v5", "", "v2"), new Value("1".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long nnzkTruss = graphulo.kTrussAdj(tA, tR, 3, true, true);
    log.info("kTruss has "+nnzkTruss+" nnz");

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(10, nnzkTruss);
    Assert.assertEquals(expect, actual);
  }


  @Test
  public void testkTrussEdge() throws TableNotFoundException {
    Connector conn = tester.getConnector();
    final String tE, tET, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tE = names[0];
      tET = names[1];
      tR = names[2];
      tRT = names[3];
    }

    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("e1", "", "v1"), new Value("1".getBytes()));
      input.put(new Key("e1", "", "v2"), new Value("1".getBytes()));
      input.put(new Key("e2", "", "v2"), new Value("1".getBytes()));
      input.put(new Key("e2", "", "v3"), new Value("1".getBytes()));
      input.put(new Key("e3", "", "v1"), new Value("1".getBytes()));
      input.put(new Key("e3", "", "v4"), new Value("1".getBytes()));
      input.put(new Key("e4", "", "v3"), new Value("1".getBytes()));
      input.put(new Key("e4", "", "v4"), new Value("1".getBytes()));
      input.put(new Key("e5", "", "v1"), new Value("1".getBytes()));
      input.put(new Key("e5", "", "v3"), new Value("1".getBytes()));
      expect.putAll(input);
      expectTranspose.putAll(TestUtil.transposeMap(expect));
      input.put(new Key("e6", "", "v2"), new Value("1".getBytes()));
      input.put(new Key("e6", "", "v5"), new Value("1".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("e22"));
      TestUtil.createTestTable(conn, tE, splits, input);
      splits.clear();
      splits.add(new Text("v22"));
      TestUtil.createTestTable(conn, tET, splits, TestUtil.transposeMap(input));
    }

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long nnzkTruss = graphulo.kTrussEdge(tE, tET, tR, tRT, 3, true, true);
    log.info("kTruss has "+nnzkTruss+" nnz");

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expect, actual);
    Assert.assertEquals(10, nnzkTruss);

    scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actualTranspose.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expectTranspose, actualTranspose);
    Assert.assertEquals(10, nnzkTruss);
  }

  @Test
  public void testJaccard() throws TableNotFoundException {
    Connector conn = tester.getConnector();
    final String tA, tADeg, tR;
    {
      String[] names = getUniqueNames(3);
      tA = names[0];
      tADeg = names[1];
      tR = names[2];
    }

    Map<Key,Double> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v1", "", "v2"), new Value("1".getBytes()));
      input.put(new Key("v1", "", "v3"), new Value("1".getBytes()));
      input.put(new Key("v1", "", "v4"), new Value("1".getBytes()));
      input.put(new Key("v2", "", "v3"), new Value("1".getBytes()));
      input.put(new Key("v3", "", "v4"), new Value("1".getBytes()));
      input.putAll(TestUtil.transposeMap(input));
      input.put(new Key("v2", "", "v5"), new Value("1".getBytes()));
      input.put(new Key("v5", "", "v2"), new Value("1".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);

      input.clear();
      input.put(new Key("v1", "", "deg"), new Value("3".getBytes()));
      input.put(new Key("v2", "", "deg"), new Value("3".getBytes()));
      input.put(new Key("v3", "", "deg"), new Value("3".getBytes()));
      input.put(new Key("v4", "", "deg"), new Value("2".getBytes()));
      input.put(new Key("v5", "", "deg"), new Value("1".getBytes()));
      TestUtil.createTestTable(conn, tADeg, splits, input);

      expect.put(new Key("v1", "", "v2"), 0.2);
      expect.put(new Key("v1", "", "v3"), 0.5);
      expect.put(new Key("v1", "", "v4"), 0.25);
      expect.put(new Key("v1", "", "v5"), 1.0/3.0);
      expect.put(new Key("v2", "", "v3"), 0.2);
      expect.put(new Key("v2", "", "v4"), 2.0/3.0);
      expect.put(new Key("v3", "", "v4"), 0.25);
      expect.put(new Key("v3", "", "v5"), 1.0/3.0);
    }

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long nnzJaccard = graphulo.Jaccard(tA, tADeg, tR, true);
    log.info("Jaccard table has "+nnzJaccard+" nnz");

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), Double.valueOf(entry.getValue().toString()));
    }
    scanner.close();
    System.out.println("Jaccard test:");
    TestUtil.printExpectActual(expect, actual);
    Assert.assertEquals(10, nnzJaccard);
    // need to be careful about comparing doubles
    for (Map.Entry<Key, Double> actualEntry : actual.entrySet()) {
      double actualValue = actualEntry.getValue();
      Assert.assertTrue(expect.containsKey(actualEntry.getKey()));
      double expectValue = expect.get(actualEntry.getKey());
      Assert.assertEquals(expectValue, actualValue, 0.001);
    }
  }

}
