package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test RemoteSourceIterator and RemoteMergeIterator.
 */
public class RemoteIteratorTest {
    private static final Logger log = LogManager.getLogger(RemoteIteratorTest.class);

    /** This is setup once for the entire class. */
    @ClassRule
    public static IAccumuloTester tester = TEST_CONFIG.AccumuloTester;

    @Test
    public void testSource() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        Connector conn = tester.getConnector();

        final String tableName = "test_"+RemoteIteratorTest.class.getSimpleName()+"_testSource";
        {
            List<Pair<Key, Value>> list = new ArrayList<>();
            list.add(new Pair<>(new Key("ccc", "", "cq"), new Value("7".getBytes())));
            list.add(new Pair<>(new Key("ddd", "", "cq"), new Value("7".getBytes())));
            list.add(new Pair<>(new Key("pogo", "", "cq"), new Value("7".getBytes())));
            list.add(new Pair<>(new Key("ddd", "", "cq2"), new Value("8".getBytes())));
            list.add(new Pair<>(new Key("ggg", "", "cq2"), new Value("8".getBytes())));
            list.add(new Pair<>(new Key("pogo", "", "cq2"), new Value("8".getBytes())));
            list.add(new Pair<>(new Key("xyz", "", "cq2"), new Value("8".getBytes())));
            TestUtil.createTestTable(conn, tableName, list);
        }

        final String tableName2 = "test_"+RemoteIteratorTest.class.getSimpleName()+"_testSource_2";
        TestUtil.createTestTable(conn, tableName2, null);

        Scanner scanner = conn.createScanner(tableName2, Authorizations.EMPTY);
        Map<String,String> itprops = new HashMap<>();
        itprops.put("instanceName",conn.getInstance().getInstanceName());
        itprops.put("tableName",tableName);
        itprops.put("zookeeperHost",conn.getInstance().getZooKeepers());
        //itprops.put("timeout","5000");
        itprops.put("username",tester.getUsername());
        itprops.put("password",new String(tester.getPassword().getPassword()));
        itprops.put("doWholeRow","true"); // *
        IteratorSetting itset = new IteratorSetting(5, RemoteSourceIterator.class, itprops); //"edu.mit.ll.graphulo.RemoteSourceIterator", itprops);
        scanner.addScanIterator(itset);
        log.info("Results of scan on table "+tableName2+" remote to "+tableName+':');
        for (Map.Entry<Key, Value> entry : scanner) {
            log.info(entry);
            log.info("decoded: "+ WholeRowIterator.decodeRow(entry.getKey(), entry.getValue())); // *
        }

        conn.tableOperations().delete(tableName);
        conn.tableOperations().delete(tableName2);
    }

    @Test
    public void testSourceSubset() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        Connector conn = tester.getConnector();

        final String tableName = "test_"+RemoteIteratorTest.class.getSimpleName()+"_testSource";
        {
            List<Pair<Key, Value>> list = new ArrayList<>();
            list.add(new Pair<>(new Key("ccc", "", "cq"), new Value("7".getBytes())));
            list.add(new Pair<>(new Key("ddd", "", "cq"), new Value("7".getBytes())));
            list.add(new Pair<>(new Key("pogo", "", "cq"), new Value("7".getBytes())));
            list.add(new Pair<>(new Key("ddd", "", "cq2"), new Value("8".getBytes())));
            list.add(new Pair<>(new Key("ggg", "", "cq2"), new Value("8".getBytes())));
            list.add(new Pair<>(new Key("pogo", "", "cq2"), new Value("8".getBytes())));
            list.add(new Pair<>(new Key("xyz", "", "cq2"), new Value("8".getBytes())));
            TestUtil.createTestTable(conn, tableName, list);
        }

        final String tableName2 = "test_"+RemoteIteratorTest.class.getSimpleName()+"_testSource_2";
        TestUtil.createTestTable(conn, tableName2, null);

        Scanner scanner = conn.createScanner(tableName2, Authorizations.EMPTY);
        Map<String,String> itprops = new HashMap<>();
        itprops.put("instanceName",conn.getInstance().getInstanceName());
        itprops.put("tableName",tableName);
        itprops.put("zookeeperHost",conn.getInstance().getZooKeepers());
        //itprops.put("timeout","5000");
        itprops.put("username",tester.getUsername());
        itprops.put("password",new String(tester.getPassword().getPassword()));
        itprops.put("doWholeRow","true"); // *
        IteratorSetting itset = new IteratorSetting(5, RemoteSourceIterator.class, itprops); //"edu.mit.ll.graphulo.RemoteSourceIterator", itprops);
        scanner.addScanIterator(itset);

        Range range = new Range("ddd",null);
        scanner.setRange(range);

        log.info("Results of scan with range "+range+" on table "+tableName2+" remote to "+tableName+':');
        for (Map.Entry<Key, Value> entry : scanner) {
            log.info(entry);
            log.info("decoded: "+ WholeRowIterator.decodeRow(entry.getKey(), entry.getValue())); // *
        }

        conn.tableOperations().delete(tableName);
        conn.tableOperations().delete(tableName2);
    }

    @Test
    public void testMerge() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException, IOException {
        Connector conn = tester.getConnector();

        final String tableName = "test_"+RemoteIteratorTest.class.getSimpleName()+"_testMerge";
        {
            List<Pair<Key, Value>> list = new ArrayList<>();
            list.add(new Pair<>(new Key("ccc", "", "cq"), new Value("7".getBytes())));
            list.add(new Pair<>(new Key("ddd", "", "cq"), new Value("7".getBytes())));
            list.add(new Pair<>(new Key("pogo", "", "cq"), new Value("7".getBytes())));
            TestUtil.createTestTable(conn, tableName, list);
        }

        final String tableName2 = "test_"+RemoteIteratorTest.class.getSimpleName()+"_testMerge_2";
        {
            List<Pair<Key, Value>> list = new ArrayList<>();
            list.add(new Pair<>(new Key("ddd", "", "cq2"), new Value("8".getBytes())));
            list.add(new Pair<>(new Key("ggg", "", "cq2"), new Value("8".getBytes())));
            list.add(new Pair<>(new Key("pogo", "", "cq2"), new Value("8".getBytes())));
            list.add(new Pair<>(new Key("xyz", "", "cq2"), new Value("8".getBytes())));
            TestUtil.createTestTable(conn, tableName2, list);
        }

        Scanner scanner = conn.createScanner(tableName2, Authorizations.EMPTY);
        Map<String,String> itprops = new HashMap<>();
        itprops.put(RemoteMergeIterator.PREFIX_RemoteIterator+"instanceName",conn.getInstance().getInstanceName());
        itprops.put(RemoteMergeIterator.PREFIX_RemoteIterator+"tableName",tableName);
        itprops.put(RemoteMergeIterator.PREFIX_RemoteIterator+"zookeeperHost",conn.getInstance().getZooKeepers());
        //itprops.put(RemoteMergeIterator.PREFIX_RemoteIterator+"timeout","5000");
        itprops.put(RemoteMergeIterator.PREFIX_RemoteIterator+"username",tester.getUsername());
        itprops.put(RemoteMergeIterator.PREFIX_RemoteIterator+"password",new String(tester.getPassword().getPassword()));
        //itprops.put(RemoteMergeIterator.PREFIX_RemoteIterator+"doWholeRow","true"); // *
        IteratorSetting itset = new IteratorSetting(5, RemoteMergeIterator.class, itprops); //"edu.mit.ll.graphulo.RemoteSourceIterator", itprops);
        scanner.addScanIterator(itset);
        log.info("Results of scan on table "+tableName2+" remote to "+tableName+':');
        for (Map.Entry<Key, Value> entry : scanner) {
            log.info(entry);
            //log.info("decoded: "+ WholeRowIterator.decodeRow(entry.getKey(), entry.getValue())); // *
        }

        conn.tableOperations().delete(tableName);
        conn.tableOperations().delete(tableName2);
    }

}
