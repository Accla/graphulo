package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.AccumuloTestBase;
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
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Testing {@link Graphulo} line graph operations.
 */
public class LineTest extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(BFSTest.class);

  /**
   *
   */
  @Test
  public void testLineUndirectedSimple() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tAT, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tA = names[0];
      tAT = names[1];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("a", "", "b"), new Value("2".getBytes()));
      input.put(new Key("b", "", "c"), new Value("4".getBytes()));
      input.putAll(TestUtil.transposeMap(input)); // undirected example
      SortedSet<Text> splits = new TreeSet<>();
//      splits.add(new Text("b"));
      TestUtil.createTestTable(conn, tA, splits, input);

      input = TestUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tAT, null, input);
    }

    {
      expect.put(new Key("a|b", "", "b|c"), new Value("3.0".getBytes()));
      expect.putAll(TestUtil.transposeMap(expect));
      expectTranspose.putAll(TestUtil.transposeMap(expect));
    }

    IteratorSetting sumSetting = new IteratorSetting(6, BigDecimalCombiner.BigDecimalSummingCombiner.class);
    Combiner.setCombineAllColumns(sumSetting, true);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    graphulo.LineGraph(tA, tAT, tR, tRT, false, "|", sumSetting,
        null, null, null, -1, true);

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    scanner.close();

    Iterator<Map.Entry<Key, Value>> acit = actual.entrySet().iterator(),
        exit = expect.entrySet().iterator();
    while (acit.hasNext() || exit.hasNext()) {
      if (acit.hasNext() && exit.hasNext())
        System.out.printf("%-51s  %s\n", exit.next(), acit.next());
      else if (acit.hasNext())
        System.out.printf("%-51s  %s\n", "", acit.next());
      else
        System.out.printf("%s\n", exit.next());
    }

    Assert.assertEquals(expect, actual);

    scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actualTranspose.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expectTranspose, actualTranspose);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tAT);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }


  /**
   *
   */
  @Test
  public void testLineDirectedSimple() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tAT, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tA = names[0];
      tAT = names[1];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("a", "", "b"), new Value("2".getBytes()));
      input.put(new Key("b", "", "c"), new Value("4".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
//      splits.add(new Text("b"));
      TestUtil.createTestTable(conn, tA, splits, input);

      input = TestUtil.transposeMap(input);
      TestUtil.createTestTable(conn, tAT, null, input);
    }

    {
      expect.put(new Key("a|b", "", "b|c"), new Value("2.0".getBytes()));
      expectTranspose.putAll(TestUtil.transposeMap(expect));
    }

    IteratorSetting sumSetting = new IteratorSetting(6, BigDecimalCombiner.BigDecimalSummingCombiner.class);
    Combiner.setCombineAllColumns(sumSetting, true);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    graphulo.LineGraph(tA, tAT, tR, tRT, true, "|", sumSetting,
        null, null, null, -1, true);

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    scanner.close();

    Iterator<Map.Entry<Key, Value>> acit = actual.entrySet().iterator(),
        exit = expect.entrySet().iterator();
    while (acit.hasNext() || exit.hasNext()) {
      if (acit.hasNext() && exit.hasNext())
        System.out.printf("%-51s  %s\n", exit.next(), acit.next());
      else if (acit.hasNext())
        System.out.printf("%-51s  %s\n", "", acit.next());
      else
        System.out.printf("%s\n", exit.next());
    }

    Assert.assertEquals(expect, actual);

    scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actualTranspose.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    Assert.assertEquals(expectTranspose, actualTranspose);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tAT);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

  /**
   *
   */
  @Test
  public void testLineUndirected() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tAT, tR, tRT;
    {
      String[] names = getUniqueNames(4);
      tA = names[0];
      tAT = names[1];
      tR = names[2];
      tRT = names[3];
    }
    Map<Key,Double> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        expectTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actualTranspose = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("a", "", "b"), new Value("4".getBytes()));
      input.put(new Key("a", "", "e"), new Value("10".getBytes()));
      input.put(new Key("b", "", "c"), new Value("5".getBytes()));
      input.put(new Key("b", "", "d"), new Value("6".getBytes()));
      input.put(new Key("c", "", "d"), new Value("3".getBytes()));
      input.putAll(TestUtil.transposeMap(input)); // undirected example
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("b"));
      TestUtil.createTestTable(conn, tA, splits, input);

      input = TestUtil.transposeMap(input);
      splits = new TreeSet<>();
      splits.add(new Text("c"));
      TestUtil.createTestTable(conn, tAT, splits, input);
    }

    {
      expect.put(new Key("a|b", "", "a|e"), 7.0);
      expect.put(new Key("a|b", "", "b|c"), 2.5);
      expect.put(new Key("a|b", "", "b|d"), 2.5);
      expect.put(new Key("b|c", "", "b|d"), 2.5);
      expect.put(new Key("b|c", "", "c|d"), 4.0);
      expect.put(new Key("b|d", "", "c|d"), 4.5);
      expect.putAll(TestUtil.transposeMap(expect));
      expectTranspose.putAll(TestUtil.transposeMap(expect));
    }

    IteratorSetting sumSetting = new IteratorSetting(6, BigDecimalCombiner.BigDecimalSummingCombiner.class);
    Combiner.setCombineAllColumns(sumSetting, true);

    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
    graphulo.LineGraph(tA, tAT, tR, tRT, false, "|", sumSetting,
        null, null, null, -1, true);

    BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actual.put(entry.getKey(), Double.valueOf(entry.getValue().toString()));
    }
    scanner.close();

    TestUtil.printExpectActual(expect, actual);

    Assert.assertEquals(expect, actual);

    scanner = conn.createBatchScanner(tRT, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key, Value> entry : scanner) {
      actualTranspose.put(entry.getKey(), Double.valueOf(entry.getValue().toString()));
    }
    scanner.close();
    Assert.assertEquals(expectTranspose, actualTranspose);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tAT);
    conn.tableOperations().delete(tR);
    conn.tableOperations().delete(tRT);
  }

}
