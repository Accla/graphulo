package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.skvi.CountAllIterator;
import edu.mit.ll.graphulo.skvi.RemoteWriteIterator;
import edu.mit.ll.graphulo.skvi.RowCountingIterator;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * Test CountAllIterator and RowCountingIterator
 */
public class CountTest extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(CountTest.class);

  @Test
  public void testRowCountingIterator() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA;
    {
      String[] names = getUniqueNames(1);
      tA = names[0];
    }

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("5".getBytes()));
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes()));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes()));
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes()));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes()));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }

    BatchScanner scanner = conn.createBatchScanner(tA, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    scanner.addScanIterator(new IteratorSetting(25, RowCountingIterator.class));
    long cnt = 0;
    for (Map.Entry<Key, Value> entry : scanner) {
      cnt += LongCombiner.STRING_ENCODER.decode(entry.getValue().get());
    }
    scanner.close();
    long expect = 3;
    Assert.assertEquals(expect, cnt);

    conn.tableOperations().delete(tA);
  }

  @Test
  public void testCountAllIterator() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA;
    {
      String[] names = getUniqueNames(1);
      tA = names[0];
    }

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("5".getBytes()));
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes()));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes()));
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes()));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes()));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }

    BatchScanner scanner = conn.createBatchScanner(tA, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    scanner.addScanIterator(new IteratorSetting(25, CountAllIterator.class));
    long cnt = 0;
    for (Map.Entry<Key, Value> entry : scanner) {
      cnt += LongCombiner.STRING_ENCODER.decode(entry.getValue().get());
    }
    scanner.close();
    long expect = 6;
    Assert.assertEquals(expect, cnt);

    conn.tableOperations().delete(tA);
  }


  @Test
  public void testDynamicIteratorOk() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
    Connector conn = tester.getConnector();
    final String tA, tB;
    {
      String[] names = getUniqueNames(2);
      tA = names[0];
      tB = names[1];
    }

    {
      Map<Key, Value> input = new HashMap<>();
      input.put(new Key("v0", "", "v1"), new Value("5".getBytes()));
      input.put(new Key("v1", "", "v2"), new Value("2".getBytes()));
      input.put(new Key("v2", "", "v0"), new Value("4".getBytes()));
      input.put(new Key("v0", "", "vBig"), new Value("7".getBytes()));
      input.put(new Key("v1", "", "vBig"), new Value("7".getBytes()));
      input.put(new Key("v2", "", "vBig"), new Value("7".getBytes()));
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text("v15"));
      TestUtil.createTestTable(conn, tA, splits, input);
    }
    TestUtil.createTestTable(conn, tB);

    BatchScanner scanner = conn.createBatchScanner(tA, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));

    Map<String, String> opt = new HashMap<>();
    String instance = conn.getInstance().getInstanceName();
    String zookeepers = conn.getInstance().getZooKeepers();
    String user = conn.whoami();
    opt.put("zookeeperHost", zookeepers);
    opt.put("instanceName", instance);
    opt.put("tableName", tB);
    opt.put("username", user);
    opt.put("password", new String(tester.getPassword().getPassword()));

    DynamicIteratorSetting dis = new DynamicIteratorSetting();
    dis.append(new IteratorSetting(25, RowCountingIterator.class));
    dis.append(new IteratorSetting(25, RemoteWriteIterator.class, opt));
    scanner.addScanIterator(dis.toIteratorSetting(25));

    for (Map.Entry<Key, Value> entry : scanner) {
      log.debug(entry.getKey() + " -> " + entry.getValue() + " AS " + Key.toPrintableString(entry.getValue().get(), 0, entry.getValue().get().length, 40) + " RAW " + Arrays.toString(entry.getValue().get()));
      int thisEntries = RemoteWriteIterator.decodeValue(entry.getValue(), null);
      log.debug(entry.getKey() + " -> " + thisEntries + " entries processed");
    }
    scanner.close();
//    Assert.assertEquals(1, totalEntries);

    scanner = conn.createBatchScanner(tB, Authorizations.EMPTY, 2);
    scanner.setRanges(Collections.singleton(new Range()));
    long cnt = 0;
    for (Map.Entry<Key, Value> entry : scanner) {
      cnt += LongCombiner.STRING_ENCODER.decode(entry.getValue().get());
    }
    scanner.close();
    long expect = 3;
    Assert.assertEquals(expect, cnt);

    conn.tableOperations().delete(tA);
    conn.tableOperations().delete(tB);
  }

}
