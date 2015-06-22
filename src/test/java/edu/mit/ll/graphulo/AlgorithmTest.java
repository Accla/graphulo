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

}
