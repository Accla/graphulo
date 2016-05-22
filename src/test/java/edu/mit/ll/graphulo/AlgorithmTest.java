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
import org.apache.accumulo.core.trace.DistributedTrace;
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

  private enum KTrussAdjAlg { Normal, Fused, Client_Sparse, Client_Dense, Smart }

  @Test
  public void testkTrussAdj_Normal() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    testkTrussAdj_Inner(KTrussAdjAlg.Normal);
  }

  @Test
  public void testkTrussAdj_Fused() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    testkTrussAdj_Inner(KTrussAdjAlg.Fused);
  }

  @Test
  public void testkTrussAdj_Client_Dense() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    testkTrussAdj_Inner(KTrussAdjAlg.Client_Dense);
  }

  @Test
  public void testkTrussAdj_Client_Sparse() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    testkTrussAdj_Inner(KTrussAdjAlg.Client_Sparse);
  }

  @Test
  public void testkTrussAdj_Smart() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    testkTrussAdj_Inner(KTrussAdjAlg.Smart);
  }


  private void testkTrussAdj_Inner(KTrussAdjAlg alg) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
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
      input.putAll(GraphuloUtil.transposeMap(input));
      expect.putAll(input);
      input.put(new Key("v2", "", "v5"), new Value("1".getBytes()));
      input.put(new Key("v5", "", "v2"), new Value("1".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long nnzkTruss;
      switch(alg) {
        case Normal:
          nnzkTruss = graphulo.kTrussAdj(tA, tR, 3, null, true, Authorizations.EMPTY, "");
          break;
        case Fused:
          nnzkTruss = graphulo.kTrussAdj_Fused(tA, tR, 3, null, true, Authorizations.EMPTY, "");
          break;
        case Client_Sparse:
          nnzkTruss = graphulo.kTrussAdj_Client(tA, tR, 3, null, Authorizations.EMPTY, "", true, Integer.MAX_VALUE);
          break;
        case Client_Dense:
          nnzkTruss = graphulo.kTrussAdj_Client(tA, tR, 3, null, Authorizations.EMPTY, "", false, Integer.MAX_VALUE);
          break;
        case Smart:
          nnzkTruss = graphulo.kTrussAdj_Smart(tA, tR, 3, null, true, Authorizations.EMPTY, "", Integer.MAX_VALUE, null);
          break;
        default: throw new AssertionError();
      }
      log.info("3-Truss has " + nnzkTruss + " nnz");

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      if (alg == KTrussAdjAlg.Normal)
        Assert.assertEquals(10, nnzkTruss);
      Assert.assertEquals(expect, actual);
    }

    conn.tableOperations().delete(tR);
    // Now test 4-truss
    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v2", "", "v4"), new Value("1".getBytes()));
      input.put(new Key("v4", "", "v2"), new Value("1".getBytes()));
      expect.putAll(input);
      GraphuloUtil.writeEntries(conn, input, tA, false);
//      Thread.sleep(200);
    }
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long nnzkTruss;
      switch(alg) {
        case Normal:
          nnzkTruss = graphulo.kTrussAdj(tA, tR, 4, null, true, Authorizations.EMPTY, "");
          break;
        case Fused:
          nnzkTruss = graphulo.kTrussAdj_Fused(tA, tR, 4, null, true, Authorizations.EMPTY, "");
          break;
        case Client_Sparse:
          nnzkTruss = graphulo.kTrussAdj_Client(tA, tR, 4, null, Authorizations.EMPTY, "", true, Integer.MAX_VALUE);
          break;
        case Client_Dense:
          nnzkTruss = graphulo.kTrussAdj_Client(tA, tR, 4, null, Authorizations.EMPTY, "", false, Integer.MAX_VALUE);
          break;
        case Smart:
          nnzkTruss = graphulo.kTrussAdj_Smart(tA, tR, 4, null, true, Authorizations.EMPTY, "", Integer.MAX_VALUE, null);
          break;
        default: throw new AssertionError();
      }
      log.info("4-Truss has " + nnzkTruss + " nnz");

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      if (alg == KTrussAdjAlg.Normal)
        Assert.assertEquals(12, nnzkTruss);
      Assert.assertEquals(expect, actual);
    }

    conn.tableOperations().delete(tR);
    // Now test 4-truss with filter
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      String filterRowCol = "v1,:,v4,";
      long nnzkTruss;
      switch(alg) {
        case Normal:
          nnzkTruss = graphulo.kTrussAdj(tA, tR, 4, filterRowCol, true, Authorizations.EMPTY, "");
          break;
        case Fused:
          nnzkTruss = graphulo.kTrussAdj_Fused(tA, tR, 4, filterRowCol, true, Authorizations.EMPTY, "");
          break;
        case Client_Sparse:
          nnzkTruss = graphulo.kTrussAdj_Client(tA, tR, 4, filterRowCol, Authorizations.EMPTY, "", true, Integer.MAX_VALUE);
          break;
        case Client_Dense:
          nnzkTruss = graphulo.kTrussAdj_Client(tA, tR, 4, filterRowCol, Authorizations.EMPTY, "", false, Integer.MAX_VALUE);
          break;
        case Smart:
          nnzkTruss = graphulo.kTrussAdj_Smart(tA, tR, 4, null, true, Authorizations.EMPTY, "", Integer.MAX_VALUE, null);
          break;
        default: throw new AssertionError();
      }
      log.info("4-Truss has " + nnzkTruss + " nnz");

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      if (alg == KTrussAdjAlg.Normal)
        Assert.assertEquals(12, nnzkTruss);
      Assert.assertEquals(expect, actual);

      // ensure same answer after compacting
      conn.tableOperations().compact(tR, null, null, true, true);
      scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
      }
      scanner.close();
      if (alg == KTrussAdjAlg.Normal)
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
      expectTranspose.putAll(GraphuloUtil.transposeMap(expect));
      input.put(new Key("e6", "", "v2"), new Value("1".getBytes()));
      input.put(new Key("e6", "", "v5"), new Value("1".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("e22"));
      TestUtil.createTestTable(conn, tE, splits, input);
      splits.clear();
      splits.add(new Text("v22"));
      TestUtil.createTestTable(conn, tET, splits, GraphuloUtil.transposeMap(input));
    }
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long nnzkTruss = graphulo.kTrussEdge(tE, tET, tR, tRT, 3, null, true, Authorizations.EMPTY);
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
      Map<Key, Value> inputTranspose = GraphuloUtil.transposeMap(input);
      expectTranspose.putAll(inputTranspose);
      GraphuloUtil.writeEntries(conn, inputTranspose, tET, false);
    }
    {
      Graphulo graphulo = new Graphulo(conn, tester.getPassword());
      long nnzkTruss = graphulo.kTrussEdge(tE, tET, tR, tRT, 3, null, true, Authorizations.EMPTY);
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
      input.putAll(GraphuloUtil.transposeMap(input));
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
    long npp = graphulo.Jaccard(tA, tADeg, tR, null, Authorizations.EMPTY, "");
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
    final String tE, tET, tW, tWT, tH, tHT, tWH, tR;
    {
      String[] names = getUniqueNames(8);
      tE = names[0];
      tET = names[1];
      tW = names[2];
      tWT = names[3];
      tH = names[4];
      tHT = names[5];
      tWH = names[6];
      tR = names[7];
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
      TestUtil.createTestTable(conn, tET, splits, GraphuloUtil.transposeMap(input));
    }

//    DistributedTrace.enable("testNMF");

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    int maxIter = 3;
    long t = System.currentTimeMillis();
    int K = 3;
    double error = graphulo.NMF(tE, tET, tW, tWT, tH, tHT, K, maxIter, true, 0, 2);
    System.out.println("Trace is "+ org.apache.htrace.Trace.isTracing()+"; NMF time "+(System.currentTimeMillis()-t));
    log.info("NMF error " + error);

    DistributedTrace.disable();

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

//    try {
//      Thread.sleep(5000);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }

    graphulo.TableMult(tWT, tH, tWH, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.DOUBLE, "", false),
        MathTwoScalar.combinerSetting(Graphulo.PLUS_ITERATOR_BIGDECIMAL.getPriority(), null, MathTwoScalar.ScalarOp.PLUS, MathTwoScalar.ScalarType.DOUBLE, false),
        null, null, null, false, false, -1);

    System.out.println("WH:");
    scanner = conn.createScanner(tWH, Authorizations.EMPTY);
    for (Map.Entry<Key, Value> entry : scanner) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    scanner.close();


    // last test for the doHT_HHTinv method
    graphulo.doHT_HHTinv(tH, tHT, K, tR, true);


    conn.tableOperations().delete(tE);
    conn.tableOperations().delete(tET);
    conn.tableOperations().delete(tW);
    conn.tableOperations().delete(tWT);
    conn.tableOperations().delete(tH);
    conn.tableOperations().delete(tHT);
    conn.tableOperations().delete(tWH);
    conn.tableOperations().delete(tR);
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
    int maxIter = 25;
    boolean trace = false;
    long t = System.currentTimeMillis();
    double error = graphulo.NMF_Client(tE, false, tW, false, tH, false, K, maxIter, 0.0, 3);
    System.out.println("Trace is " + trace + "; Client NMF time " + (System.currentTimeMillis() - t));
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

  /**
   * ans =
     7     6     0     0     0
     2     0     6     0     0
     3     0     0     0     1
     0     2     0     0     0
     0     0     0     9     1
     2     0     1     4     0
     ans =
     0.4246    0.5071         0         0         0
     0.1971         0    0.8240         0         0
     0.5913         0         0         0    0.2747
     0    1.0986         0         0         0
     0         0         0    0.9888    0.1099
     0.2253         0    0.1569    0.6278         0

   */
  @Test
  public void testTfidf() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Connector conn = tester.getConnector();
    final String tEDeg, tET, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tET = names[0];
      tEDeg = names[1];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Double> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    Map<Key, Value> input = new HashMap<>(), indeg = new HashMap<>();
    {
      input.put(new Key("e1", "", "v1"), new Value("7".getBytes()));
      input.put(new Key("e1", "", "v2"), new Value("6".getBytes()));
      input.put(new Key("e2", "", "v1"), new Value("2".getBytes()));
      input.put(new Key("e2", "", "v3"), new Value("6".getBytes()));
      input.put(new Key("e3", "", "v1"), new Value("3".getBytes()));
      input.put(new Key("e3", "", "v5"), new Value("1".getBytes()));
      input.put(new Key("e4", "", "v2"), new Value("2".getBytes()));
      input.put(new Key("e5", "", "v4"), new Value("9".getBytes()));
      input.put(new Key("e5", "", "v5"), new Value("1".getBytes()));
      input.put(new Key("e6", "", "v1"), new Value("2".getBytes()));
      input.put(new Key("e6", "", "v3"), new Value("1".getBytes()));
      input.put(new Key("e6", "", "v4"), new Value("4".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v22"));
      TestUtil.createTestTable(conn, tET, splits, GraphuloUtil.transposeMap(input));

      indeg.put(new Key("e1", "", "Degree"), new Value("13".getBytes()));
      indeg.put(new Key("e2", "", "Degree"), new Value("8".getBytes()));
      indeg.put(new Key("e3", "", "Degree"), new Value("4".getBytes()));
      indeg.put(new Key("e4", "", "Degree"), new Value("2".getBytes()));
      indeg.put(new Key("e5", "", "Degree"), new Value("10".getBytes()));
      indeg.put(new Key("e6", "", "Degree"), new Value("7".getBytes()));
      TestUtil.createTestTable(conn, tEDeg, null, indeg);

      expect.put(new Key("e1", "", "v1"), 0.4246);
      expect.put(new Key("e1", "", "v2"), 0.5071);
      expect.put(new Key("e2", "", "v1"), 0.1971);
      expect.put(new Key("e2", "", "v3"), 0.8240);
      expect.put(new Key("e3", "", "v1"), 0.5913);
      expect.put(new Key("e3", "", "v5"), 0.2747);
      expect.put(new Key("e4", "", "v2"), 1.0986);
      expect.put(new Key("e5", "", "v4"), 0.9888);
      expect.put(new Key("e5", "", "v5"), 0.1099);
      expect.put(new Key("e6", "", "v1"), 0.2253);
      expect.put(new Key("e6", "", "v3"), 0.1569);
      expect.put(new Key("e6", "", "v4"), 0.6278);
    }

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    long numEntriesResultTable = graphulo.doTfidf(tET, tEDeg, -1, tR, tRT);


    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), Double.valueOf(entry.getValue().toString()));
    }
    scanner.close();
    System.out.println("Tfidf test:");
    TestUtil.printExpectActual(expect, actual);
    Assert.assertEquals(input.size(), numEntriesResultTable);
    // need to be careful about comparing doubles
    for (Map.Entry<Key, Double> actualEntry : actual.entrySet()) {
      double actualValue = actualEntry.getValue();
      Assert.assertTrue(expect.containsKey(actualEntry.getKey()));
      double expectValue = expect.get(actualEntry.getKey());
      Assert.assertEquals(expectValue, actualValue, 0.001);
    }

    conn.tableOperations().delete(tET);
    conn.tableOperations().delete(tEDeg);
    conn.tableOperations().delete(tRT);
    conn.tableOperations().delete(tR);
  }

}
