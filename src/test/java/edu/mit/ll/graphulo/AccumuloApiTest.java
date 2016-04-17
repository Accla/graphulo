package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static edu.mit.ll.graphulo.Graphulo.DEFAULT_COMBINER_PRIORITY;

/**
 * Test features of the Accumulo API that Graphulo relies on.
 */
public class AccumuloApiTest extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(AccumuloApiTest.class);

  /**
   * Tests Accumulo API to make sure Combiners pick up writes to the same key with same and differing timestamps,
   * in the same batch and across batches.
   */
  @Test
  public void testCombineSameWrites() throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    Connector conn = tester.getConnector();
    final String tR;
    {
      String[] names = getUniqueNames(1);
      tR = names[0];
    }

    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);
    expect.put(new Key("v2", "", "v5"), new Value("1800".getBytes()));
    IteratorSetting RPlusIteratorSetting = new DynamicIteratorSetting(DEFAULT_COMBINER_PRIORITY, null)
        .append(MathTwoScalar.combinerSetting(1, null, MathTwoScalar.ScalarOp.PLUS, MathTwoScalar.ScalarType.LONG_OR_DOUBLE, false))
        .toIteratorSetting();

    // first test writing different timestamps. Some can be same.
    TestUtil.createTestTable(conn, tR);
    GraphuloUtil.applyIteratorSoft(RPlusIteratorSetting, conn.tableOperations(), tR);
    {
      BatchWriter bw = conn.createBatchWriter(tR, new BatchWriterConfig());
      Key k = new Key("v2", "", "v5");
      Value v = new Value("1".getBytes());
      Mutation m;

      for (int i = 0; i < 1800; i++) {
        m = new Mutation(k.getRowData().toArray());
        m.put(k.getColumnFamilyData().toArray(), k.getColumnQualifierData().toArray(),
            k.getColumnVisibilityParsed(), 20+((int)(Math.random()*100)), v.get());
        bw.addMutation(m);
        if (Math.random() < 0.2)
          bw.flush();
      }
      bw.close();

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
        log.debug("Entry "+actual.size()+": "+entry.getKey()+"    "+entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);
    }

    // now test writing all same timestamp
    conn.tableOperations().delete(tR);
    TestUtil.createTestTable(conn, tR);
    GraphuloUtil.applyIteratorSoft(RPlusIteratorSetting, conn.tableOperations(), tR);
    {
      BatchWriter bw = conn.createBatchWriter(tR, new BatchWriterConfig());
      Key k = new Key("v2", "", "v5");
      Value v = new Value("1".getBytes());
      Mutation m;

      for (int i = 0; i < 1800; i++) {
        m = new Mutation(k.getRowData().toArray());
        m.put(k.getColumnFamilyData().toArray(), k.getColumnQualifierData().toArray(),
            k.getColumnVisibilityParsed(), 9, v.get());
        bw.addMutation(m);
        if (Math.random() < 0.1)
          bw.flush();
      }
      bw.close();

      BatchScanner scanner = conn.createBatchScanner(tR, Authorizations.EMPTY, 2);
      scanner.setRanges(Collections.singleton(new Range()));
      for (Map.Entry<Key, Value> entry : scanner) {
        actual.put(entry.getKey(), entry.getValue());
        log.debug("Entry "+actual.size()+": "+entry.getKey()+"    "+entry.getValue());
      }
      scanner.close();
      Assert.assertEquals(expect, actual);
    }

    conn.tableOperations().delete(tR);
  }

}
