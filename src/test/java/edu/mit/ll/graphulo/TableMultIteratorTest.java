package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * Test DotMultIterator.
 */
public class TableMultIteratorTest {
    private static final Logger log = LogManager.getLogger(TableMultIteratorTest.class);

    /** This is setup once for the entire class. */
    @ClassRule
    public static IAccumuloTester tester = TEST_CONFIG.AccumuloTester;

    @Test
    public void testSortedMapComparator() {
        Key k1 = new Key("row1","colF1","colQ1");
        Key k2 = new Key("row2","colF1","colQ1");
        Key k3 = new Key("row3","colF1","colQ1");

        SortedMap<Key,Integer> map = new TreeMap<>(new DotIterator.ColFamilyQualifierComparator());
        map.put(k1, 1);
        map.put(k2, 2);
        int v = map.get(k3);
        Assert.assertEquals(2,v);

        //log.info("map returned: "+v);
    }

    @Test
    public void testSplitMapPrefix() {
        Map<String,String> map = new HashMap<>();
        map.put("A.bla","123");
        map.put("A.bla2","345");
        map.put("B.ok","789");
        map.put("plain","vanilla");

        Map<String, Map<String, String>> expect = new HashMap<>();
        Map<String,String> m1 = new HashMap<>();
        m1.put("bla", "123");
        m1.put("bla2", "345");
        expect.put("A", m1);
        expect.put("B", Collections.singletonMap("ok", "789"));
        expect.put("", Collections.singletonMap("plain", "vanilla"));

        Map<String, Map<String, String>> actual = GraphuloUtil.splitMapPrefix(map);
        Assert.assertEquals(expect, actual);
    }



    /**
     * <pre>
     *      C1 C2        C1 C2 C3          B1  B2
     * A1 [ 2  2 ] * B1 [   3  3  ] = A1 [ 6   6,6 ]
     * A2 [ 2    ]   B2 [3  3     ]   A2 [     6   ]
     * </pre>
     */
    @Test
    public void testDotMultIteratorScan() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        Connector conn = tester.getConnector();

        final String tableNameA = "test_"+TableMultIteratorTest.class.getSimpleName()+"_testDotMultIteratorScan_A";
        {
            Map<Key, Value> input = new HashMap<>();
            input.put(new Key("A1", "", "C1"), new Value("2".getBytes()));
            input.put(new Key("A1", "", "C2"), new Value("2".getBytes()));
            input.put(new Key("A2", "", "C1"), new Value("2".getBytes()));
            input = TestUtil.tranposeMap(input);
            TestUtil.createTestTable(conn, tableNameA, null, input);
        }

        final String tableNameBT = "test_"+TableMultIteratorTest.class.getSimpleName()+"_testDotMultIteratorScan_BT";
        {
            Map<Key, Value> input = new HashMap<>();
            input.put(new Key("B1", "", "C2"), new Value("3".getBytes()));
            input.put(new Key("B1", "", "C3"), new Value("3".getBytes()));
            input.put(new Key("B2", "", "C1"), new Value("3".getBytes()));
            input.put(new Key("B2", "", "C2"), new Value("3".getBytes()));
            input = TestUtil.tranposeMap(input);
            TestUtil.createTestTable(conn, tableNameBT, null, input);
        }

        final String tableNameC = "test_"+TableMultIteratorTest.class.getSimpleName()+"_testDotMultIteratorScan_C";
        TestUtil.createTestTable(conn, tableNameC, null);

        Scanner scanner = conn.createScanner(tableNameC, Authorizations.EMPTY);
        Map<String,String> itprops = new HashMap<>();
        itprops.put("A.instanceName",conn.getInstance().getInstanceName());
        itprops.put("A.tableName",tableNameA);
        itprops.put("A.zookeeperHost",conn.getInstance().getZooKeepers());
        //itprops.put("A.timeout","5000");
        itprops.put("A.username",tester.getUsername());
        itprops.put("A.password",new String(tester.getPassword().getPassword()));
        //itprops.put("A.doWholeRow","true"); // *
        itprops.put("BT.instanceName",conn.getInstance().getInstanceName());
        itprops.put("BT.tableName",tableNameBT);
        itprops.put("BT.zookeeperHost",conn.getInstance().getZooKeepers());
        //itprops.put("BT.timeout","5000");
        itprops.put("BT.username",tester.getUsername());
        itprops.put("BT.password",new String(tester.getPassword().getPassword()));
        //itprops.put("BT.doWholeRow","true"); // *
        IteratorSetting itset = new IteratorSetting(25, DotIterator.class, itprops);
        scanner.addScanIterator(itset);
//        scanner.addScanIterator(new IteratorSetting(26, DebugIterator.class, Collections.<String,String>emptyMap()));

        // compact
//        List<IteratorSetting> listset = new ArrayList<>();
//        listset.add(itset);
//        listset.add(new IteratorSetting(26, DebugInfoIterator.class, Collections.<String, String>emptyMap()));
//        conn.tableOperations().compact(tableNameC, null, null, listset, true, true); // block

        Map<Key,Value> expect = new HashMap<>();
        expect.put(new Key("A1", "", "B2"), new Value("6".getBytes()));
        expect.put(new Key("A1", "", "B1"), new Value("6".getBytes()));
        expect.put(new Key("A1", "", "B2"), new Value("6".getBytes()));
        expect.put(new Key("A2", "", "B2"), new Value("6".getBytes()));
        Map<Key, Value> actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ); // only compare row, colF, colQ

        log.info("Results of scan on table " + tableNameC + " with A=" + tableNameA + " and BT="+tableNameBT+':');
        for (Map.Entry<Key, Value> entry : scanner) {
            log.info(entry);
            actual.put(entry.getKey(), entry.getValue());
        }
        Assert.assertEquals(expect, actual);

        conn.tableOperations().delete(tableNameA);
        conn.tableOperations().delete(tableNameBT);
        conn.tableOperations().delete(tableNameC);
    }

    /**
     * <pre>
     *      C1 C2        C1 C2 C3          B1          B1  B2
     * A1 [ 2  2 ] * B1 [   3  3  ] + A1 [ 1  ] = A1 [ 7   12 ]
     * A2 [ 2    ]   B2 [3  3     ]               A2 [     6  ]
     * </pre>
     */
    @Ignore("New version only works with BatchWriter")
    @Test
    public void testTableMultIterator() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        Connector conn = tester.getConnector();
        final String tablePrefix = "testTableMult_";

        final String tableNameA = tablePrefix+"A";
        {
            Map<Key, Value> input = new HashMap<>();
            input.put(new Key("A1", "", "C1"), new Value("2".getBytes()));
            input.put(new Key("A1", "", "C2"), new Value("2".getBytes()));
            input.put(new Key("A2", "", "C1"), new Value("2".getBytes()));
            TestUtil.createTestTable(conn, tableNameA, null, input);
        }
        SortedSet<Text> splitSet = new TreeSet<>();
        splitSet.add(new Text("A15"));
        //conn.tableOperations().addSplits(tableNameA, splitSet);

        final String tableNameBT = tablePrefix+"BT";
        {
            Map<Key, Value> input = new HashMap<>();
            input.put(new Key("B1", "", "C2"), new Value("3".getBytes()));
            input.put(new Key("B1", "", "C3"), new Value("3".getBytes()));
            input.put(new Key("B2", "", "C1"), new Value("3".getBytes()));
            input.put(new Key("B2", "", "C2"), new Value("3".getBytes()));
            TestUtil.createTestTable(conn, tableNameBT, null, input);
        }

        final String tableNameC = tablePrefix+"C";
        {
            Map<Key, Value> input = new HashMap<>();
            input.put(new Key("A1", "", "B1"), new Value("1".getBytes()));

            Key testkey = new Key("zr", "", "zc");
            testkey.setDeleted(true);
            input.put(testkey, null);
            //list.put(new Key("zr", "", "zc"), new Value("100".getBytes())));


            TestUtil.createTestTable(conn, tableNameC, splitSet, input);
        }

        Scanner scanner = conn.createScanner(tableNameC, Authorizations.EMPTY);
        Map<String,String> itprops = new HashMap<>();
        itprops.put("A.instanceName",conn.getInstance().getInstanceName());
        itprops.put("A.tableName",tableNameA);
        itprops.put("A.zookeeperHost",conn.getInstance().getZooKeepers());
        //itprops.put("A.timeout","5000");
        itprops.put("A.username",tester.getUsername());
        itprops.put("A.password",new String(tester.getPassword().getPassword()));
        //itprops.put("A.doWholeRow","true"); // *
        itprops.put("BT.instanceName",conn.getInstance().getInstanceName());
        itprops.put("BT.tableName",tableNameBT);
        itprops.put("BT.zookeeperHost",conn.getInstance().getZooKeepers());
        //itprops.put("BT.timeout","5000");
        itprops.put("BT.username",tester.getUsername());
        itprops.put("BT.password",new String(tester.getPassword().getPassword()));
        //itprops.put("BT.doWholeRow","true"); // *
        IteratorSetting itset = new IteratorSetting(15, TableMultIterator.class, itprops);

        //scanner.addScanIterator(new IteratorSetting(16, DebugInfoIterator.class, Collections.<String,String>emptyMap()));

        // compact
//        List<IteratorSetting> listset = new ArrayList<>();
//        listset.add(itset);
//        listset.add(new IteratorSetting(26, DebugInfoIterator.class, Collections.<String, String>emptyMap()));
//        StopWatch sw = new StopWatch();
//        sw.start();
//        conn.tableOperations().compact(tableNameC, null, null, listset, true, true); // block
//        sw.stop();
//        log.info("compaction took "+sw.getTime()/1000.0+"s");


        Map<Key,Value> expect = new HashMap<>();
        expect.put(new Key("A1", "", "B1"), new Value("7".getBytes()));
        expect.put(new Key("A1", "", "B2"), new Value("12".getBytes()));
        expect.put(new Key("A2", "", "B2"), new Value("6".getBytes()));

        // first test on scan scope
        scanner.addScanIterator(itset);
        scanner.addScanIterator(new IteratorSetting(16, DebugInfoIterator.class, Collections.<String, String>emptyMap()));
        {
            Map<Key, Value> actual = new HashMap<>();
            log.info("Scanning with TableMultIterator:");
            for (Map.Entry<Key, Value> entry : scanner) {
                log.info(entry);
                Key k = entry.getKey(); // don't copy vis or timestamp; we don't care about comparing those
                actual.put(new Key(k.getRow(), k.getColumnFamily(), k.getColumnQualifier()), entry.getValue());
            }
            Assert.assertEquals(expect,actual);
            scanner.clearScanIterators();
        }

        // now test on compact
        List<IteratorSetting> listset = new ArrayList<>();
        listset.add(itset);
        StopWatch sw = new StopWatch();
        sw.start();
        conn.tableOperations().compact(tableNameC, null, null, listset, true, true); // block
        sw.stop();
        log.info("compaction took " + sw.getTime() / 1000.0 + "s");

        {
            Map<Key, Value> actual = new HashMap<>();
            log.info("Scan after compact with TableMultIterator:");
            for (Map.Entry<Key, Value> entry : scanner) {
                log.info(entry);
                Key k = entry.getKey(); // don't copy vis or timestamp; we don't care about comparing those
                actual.put(new Key(k.getRow(), k.getColumnFamily(), k.getColumnQualifier()), entry.getValue());
            }
            Assert.assertEquals(expect, actual);
        }

        // now test dumping to a totally different table ---------
        // make table R
        final String tableNameZ = tablePrefix+"Z";
        {
            Map<Key, Value> input = new HashMap<>();
            input.put(new Key("A1", "", "B1"), new Value("1".getBytes()));

            Key testkey = new Key("AAA", "", "CCC");
            testkey.setDeleted(true);
            input.put(testkey, null);
            testkey = new Key("zr", "", "zc");
            testkey.setDeleted(true);
            input.put(testkey, null);

            TestUtil.createTestTable(conn, tableNameZ, splitSet, input); // Write single delete entry to trigger compaction
        }


        final String tableNameR = tablePrefix+"R";
        TestUtil.createTestTable(conn, tableNameR, null);

        itprops.put("R.instanceName",conn.getInstance().getInstanceName());
        itprops.put("R.tableName",tableNameR);
        itprops.put("R.zookeeperHost",conn.getInstance().getZooKeepers());
        //itprops.put("R.timeout","5000");
        itprops.put("R.username", tester.getUsername());
        itprops.put("R.password",new String(tester.getPassword().getPassword()));
        IteratorSetting itset2 = new IteratorSetting(15, TableMultIterator.class, itprops);

        // compact Z with iterator, writing to R
        List<IteratorSetting> listset2 = new ArrayList<>();
        listset2.add(itset2);
        sw = new StopWatch();
        sw.start();
        conn.tableOperations().compact(tableNameZ, null, null, listset2, true, true); // block
        sw.stop();
        log.info("compaction took " + sw.getTime() / 1000.0 + "s");

        expect = new HashMap<>();
        expect.put(new Key("A1", "", "B1"), new Value("7".getBytes()));
        expect.put(new Key("A1", "", "B2"), new Value("12".getBytes()));
        expect.put(new Key("A2", "", "B2"), new Value("6".getBytes()));

        scanner.close();
        scanner = conn.createScanner(tableNameR, Authorizations.EMPTY);
        {
            Map<Key, Value> actual = new HashMap<>();
            log.info("Scan R after compact on Z with TableMultIterator writing to R:");
            for (Map.Entry<Key, Value> entry : scanner) {
                log.info(entry);
                Key k = entry.getKey(); // don't copy vis or timestamp; we don't care about comparing those
                actual.put(new Key(k.getRow(), k.getColumnFamily(), k.getColumnQualifier()), entry.getValue());
            }

            Assert.assertEquals(expect, actual);
        }

        scanner.close();
        conn.tableOperations().delete(tableNameA);
        conn.tableOperations().delete(tableNameBT);
        conn.tableOperations().delete(tableNameC);
        conn.tableOperations().delete(tableNameR);
        conn.tableOperations().delete(tableNameZ);
    }

}
