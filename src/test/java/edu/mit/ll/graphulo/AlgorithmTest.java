package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
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
  public void testkTrussAdj() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
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
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long nnzkTruss = graphulo.kTrussAdj(tA, tR, 3, null, true, true);
      log.info("3Truss has " + nnzkTruss + " nnz");

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(10, nnzkTruss);
      Assert.assertEquals(expect, actual);
    }

    // Now test 4-truss
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v2", "", "v4"), new Value("1".getBytes()));
      input.put(new Key("v4", "", "v2"), new Value("1".getBytes()));
      expect.putAll(input);
      GraphuloUtil.writeEntries(conn, input, tA, false);
    }
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long nnzkTruss = graphulo.kTrussAdj(tA, tR, 4, null, true, true);
      log.info("4Truss has " + nnzkTruss + " nnz");

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(12, nnzkTruss);
      Assert.assertEquals(expect, actual);
    }


    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tR);
  }


  @Test
  public void testkTrussEdge() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
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
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long nnzkTruss = graphulo.kTrussEdge(tE, tET, tR, tRT, 3, true, true);
      log.info("3Truss has " + nnzkTruss + " nnz");

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

    // Now test 4-truss
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("e7", "", "v4"), new Value("1".getBytes()));
      input.put(new Key("e7", "", "v2"), new Value("1".getBytes()));
      expect.putAll(input);
      GraphuloUtil.writeEntries(conn, input, tE, false);
      Map<Key, Value> inputTranspose = TestUtil.transposeMap(input);
      expectTranspose.putAll(inputTranspose);
      GraphuloUtil.writeEntries(conn, inputTranspose, tET, false);
    }
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long nnzkTruss = graphulo.kTrussEdge(tE, tET, tR, tRT, 3, true, true);
      log.info("4Truss has " + nnzkTruss + " nnz");

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(12, nnzkTruss);
      Assert.assertEquals(expect, actual);

      scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actualTranspose.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expectTranspose, actualTranspose);
      Assert.assertEquals(12, nnzkTruss);
    }
    conn.tableOperations().delete(tE);
    conn.tableOperations().delete(tET);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  @Test
  public void testJaccard() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
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
      expect.put(new Key("v1", "", "v5"), 1.0 / 3.0);
      expect.put(new Key("v2", "", "v3"), 0.2);
      expect.put(new Key("v2", "", "v4"), 2.0 / 3.0);
      expect.put(new Key("v3", "", "v4"), 0.25);
      expect.put(new Key("v3", "", "v5"), 1.0 / 3.0);
    }

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long npp = graphulo.Jaccard(tA, tADeg, tR, null, true);
    log.info("Jaccard table has "+npp+" #partial products sent to "+tR);

    // Just for fun, let's compact and ensure idempotence.
    conn.tableOperations().compact(tR, null, null, true, true);

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), Double.valueOf(entry.getValue().toString()));
    }
    scanner.close();
    System.out.println("Jaccard test:");
    TestUtil.printExpectActual(expect, actual);
    Assert.assertEquals(10, npp);
    // need to be careful about comparing doubles
    for (Map.Entry<Key, Double> actualEntry : actual.entrySet()) {
      double actualValue = actualEntry.getValue();
      Assert.assertTrue(expect.containsKey(actualEntry.getKey()));
      double expectValue = expect.get(actualEntry.getKey());
      Assert.assertEquals(expectValue, actualValue, 0.001);
    }

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tADeg);
    conn.tableOperations().delete(tR);
  }


  @Test
  public void testNMF() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Connector conn = tester.getConnector();
    final String tE, tET, tW, tWT, tH, tHT, tWH;
    {
      String[] names = getUniqueNames(7);
      tE = names[0];
      tET = names[1];
      tW = names[2];
      tWT = names[3];
      tH = names[4];
      tHT = names[5];
      tWH = names[6];
    }
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
    int maxIter = 4;
    boolean trace = false;
    long t = System.currentTimeMillis();
    double error = graphulo.NMF(tE, tET, tW, tWT, tH, tHT, 3, maxIter, true, trace);
    System.out.println("Trace is "+trace+"; NMF time "+(System.currentTimeMillis()-t));
    log.info("NMF error " + error);

    System.out.println("A:");
    Scanner scanner = conn.createScanner(tE, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    scanner.close();

    System.out.println("W:");
    scanner = conn.createScanner(tW, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    scanner.close();

    System.out.println("H:");
    scanner = conn.createScanner(tH, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    scanner.close();

    graphulo.TableMult(tWT, tH, tWH, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.DOUBLE),
        MathTwoScalar.combinerSetting(Graphulo.PLUS_ITERATOR_BIGDECIMAL.getPriority(), null, MathTwoScalar.ScalarOp.PLUS, MathTwoScalar.ScalarType.DOUBLE),
        null, null, null, false, false, -1, false);

    System.out.println("WH:");
    scanner = conn.createScanner(tWH, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    scanner.close();

    conn.tableOperations().delete(tE);
    conn.tableOperations().delete(tET);
    conn.tableOperations().delete(tW);
    conn.tableOperations().delete(tWT);
    conn.tableOperations().delete(tH);
    conn.tableOperations().delete(tHT);
    conn.tableOperations().delete(tWH);
  }

  @Test
  public void testNMF_Client() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Connector conn = tester.getConnector();
    final String tE, tET, tW, tWT, tH, tHT, tWH;
    {
      String[] names = getUniqueNames(7);
      tE = names[0];
      tW = names[2];
      tH = names[4];
//      tWH = names[6];
    }
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
      input.put(new Key("e6", "", "v2"), new Value("1".getBytes()));
      input.put(new Key("e6", "", "v5"), new Value("1".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("e22"));
      TestUtil.createTestTable(conn, tE, splits, input);
//      splits.clear();
//      splits.add(new Text("v22"));
//      TestUtil.createTestTable(conn, tET, splits, TestUtil.transposeMap(input));
    }

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    int K = 3;
    int maxIter = 10;
    boolean trace = false;
    long t = System.currentTimeMillis();
    double error = graphulo.NMF_Client(tE, tW, tH, K, maxIter, trace);
    System.out.println("Trace is "+trace+"; Client NMF time "+(System.currentTimeMillis()-t));
    log.info("NMF error " + error);

    System.out.println("A:");
    Scanner scanner = conn.createScanner(tE, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    scanner.close();

    System.out.println("W:");
    scanner = conn.createScanner(tW, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    scanner.close();

    System.out.println("H:");
    scanner = conn.createScanner(tH, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    scanner.close();

//    graphulo.TableMult(tWT, tH, tWH, null, -1,
//        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.DOUBLE),
//        MathTwoScalar.combinerSetting(Graphulo.PLUS_ITERATOR_BIGDECIMAL.getPriority(), null, MathTwoScalar.ScalarOp.PLUS, MathTwoScalar.ScalarType.DOUBLE),
//        null, null, null, false, false, -1, false);
//
//    System.out.println("WH:");
//    scanner = conn.createScanner(tWH, Authorizations.EMPTY);
//    for (Map.Entry<Key, Value> entry : scanner) {
//      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
//    }
//    scanner.close();

    conn.tableOperations().delete(tE);
//    conn.tableOperations().delete(tET);
    conn.tableOperations().delete(tW);
//    conn.tableOperations().delete(tWT);
    conn.tableOperations().delete(tH);
//    conn.tableOperations().delete(tHT);
//    conn.tableOperations().delete(tWH);
  }

}
