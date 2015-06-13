package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Test the SCC algorithm in {@link SCCGraphulo}.
 */
public class SCCTest extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(SCCTest.class);

    /**
     *      vBig
     *   /   |    \
     *  v    v     v
     * v0--->v1--->v2--v
     *  ^--<------<----/
     */
  @Test
  public void testSCC() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tRf;
    {
      String[] names = getUniqueNames(2);
      tA = names[0];
      tRf = names[1];
    }
    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ), actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    {
      Map<Key,Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("1".getBytes()));
      input.put(new Key("v1", "", "v2"), new Value("1".getBytes()));
      input.put(new Key("v2", "", "v0"), new Value("1".getBytes()));
      input.put(new Key("vBig", "", "v0"), new Value("1".getBytes()));
      input.put(new Key("vBig", "", "v1"), new Value("1".getBytes()));
      input.put(new Key("vBig", "", "v2"), new Value("1".getBytes()));

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
    }

    SCCGraphulo sccgraphulo = new SCCGraphulo(conn, tester.getPassword());
    sccgraphulo.SCC(tA, tRf, 4, true);

    BatchScanner scanner = conn.createBatchScanner(tRf, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    for (Map.Entry<Key,Value> entry : scanner) {
      actual.put(entry.getKey(), entry.getValue());
    }
    scanner.close();
    for (String s: sccgraphulo.SCCQuery(tRf,"v0,")){System.out.println(s);}
    for (String s: sccgraphulo.SCCQuery(tRf,"vBig,")){System.out.println(s);}
    for (String s: sccgraphulo.SCCQuery(tRf,null)){System.out.println(s);}

    Assert.assertEquals(expect, actual);
  }

}
