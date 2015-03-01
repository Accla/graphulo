package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.DebugIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
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

        SortedMap<Key,Integer> map = new TreeMap<>(new DotMultIterator.ColFamilyQualifierComparator());
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
            List<Pair<Key, Value>> list = new ArrayList<>();
            list.add(new Pair<>(new Key("A1", "", "C1"), new Value("2".getBytes())));
            list.add(new Pair<>(new Key("A1", "", "C2"), new Value("2".getBytes())));
            list.add(new Pair<>(new Key("A2", "", "C1"), new Value("2".getBytes())));
            TestUtil.createTestTable(conn, tableNameA, list);
        }

        final String tableNameBT = "test_"+TableMultIteratorTest.class.getSimpleName()+"_testDotMultIteratorScan_BT";
        {
            List<Pair<Key, Value>> list = new ArrayList<>();
            list.add(new Pair<>(new Key("B1", "", "C2"), new Value("3".getBytes())));
            list.add(new Pair<>(new Key("B1", "", "C3"), new Value("3".getBytes())));
            list.add(new Pair<>(new Key("B2", "", "C1"), new Value("3".getBytes())));
            list.add(new Pair<>(new Key("B2", "", "C2"), new Value("3".getBytes())));
            TestUtil.createTestTable(conn, tableNameBT, list);
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
        IteratorSetting itset = new IteratorSetting(25, DotMultIterator.class, itprops);
        scanner.addScanIterator(itset);
//        scanner.addScanIterator(new IteratorSetting(26, DebugIterator.class, Collections.<String,String>emptyMap()));

        // compact
//        List<IteratorSetting> listset = new ArrayList<>();
//        listset.add(itset);
//        listset.add(new IteratorSetting(26, DebugInfoIterator.class, Collections.<String, String>emptyMap()));
//        conn.tableOperations().compact(tableNameC, null, null, listset, true, true); // block

        log.info("Results of scan on table " + tableNameC + " with A=" + tableNameA + " and BT="+tableNameBT+':');
        for (Map.Entry<Key, Value> entry : scanner) {
            log.info(entry);
            //log.info("decoded: " + WholeRowIterator.decodeRow(entry.getKey(), entry.getValue())); // *
        }

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
    @Test
    public void testTableMultIterator() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        Connector conn = tester.getConnector();

        final String tableNameA = "test_"+TableMultIteratorTest.class.getSimpleName()+"_testTableMultIterator_A";
        {
            List<Pair<Key, Value>> list = new ArrayList<>();
            list.add(new Pair<>(new Key("A1", "", "C1"), new Value("2".getBytes())));
            list.add(new Pair<>(new Key("A1", "", "C2"), new Value("2".getBytes())));
            list.add(new Pair<>(new Key("A2", "", "C1"), new Value("2".getBytes())));
            TestUtil.createTestTable(conn, tableNameA, list);
        }

        final String tableNameBT = "test_"+TableMultIteratorTest.class.getSimpleName()+"_testTableMultIterator_BT";
        {
            List<Pair<Key, Value>> list = new ArrayList<>();
            list.add(new Pair<>(new Key("B1", "", "C2"), new Value("3".getBytes())));
            list.add(new Pair<>(new Key("B1", "", "C3"), new Value("3".getBytes())));
            list.add(new Pair<>(new Key("B2", "", "C1"), new Value("3".getBytes())));
            list.add(new Pair<>(new Key("B2", "", "C2"), new Value("3".getBytes())));
            TestUtil.createTestTable(conn, tableNameBT, list);
        }

        final String tableNameC = "test_"+TableMultIteratorTest.class.getSimpleName()+"_testTableMultIterator_C";
        {
            List<Pair<Key, Value>> list = new ArrayList<>();
            list.add(new Pair<>(new Key("A1", "", "B1"), new Value("1".getBytes())));
            TestUtil.createTestTable(conn, tableNameC, list);
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


        Set<Pair<Key,Value>> expect = new HashSet<>();
        expect.add(new Pair<>(new Key("A1", "", "B1"), new Value("7".getBytes())));
        expect.add(new Pair<>(new Key("A1", "", "B2"), new Value("12".getBytes())));
        expect.add(new Pair<>(new Key("A2", "", "B2"), new Value("6".getBytes())));

        // first test on scan scope
        scanner.addScanIterator(itset);
        {
            Set<Pair<Key, Value>> actual = new HashSet<>();
            log.info("Scanning with TableMultIterator:");
            for (Map.Entry<Key, Value> entry : scanner) {
                log.info(entry);
                Key k = entry.getKey(); // don't copy vis or timestamp; we don't care about comparing those
                actual.add(new Pair<>(new Key(k.getRow(), k.getColumnFamily(), k.getColumnQualifier()), entry.getValue()));
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
            Set<Pair<Key, Value>> actual = new HashSet<>();
            log.info("Scan after compact with TableMultIterator:");
            for (Map.Entry<Key, Value> entry : scanner) {
                log.info(entry);
                Key k = entry.getKey(); // don't copy vis or timestamp; we don't care about comparing those
                actual.add(new Pair<>(new Key(k.getRow(), k.getColumnFamily(), k.getColumnQualifier()), entry.getValue()));
            }
            Assert.assertEquals(expect, actual);
        }

        // now test dumping to a totally different table ---------
        // make table R
        final String tableNameZ = "test_"+TableMultIteratorTest.class.getSimpleName()+"_testTableMultIterator_Z";
        Key testkey = new Key("AAA", "", "CCC");
        testkey.setDeleted(true);
        TestUtil.createTestTable(conn, tableNameZ, Collections.singleton(new Pair<Key,Value>(testkey, null))); // Write single delete entry to trigger compaction
        final String tableNameR = "test_"+TableMultIteratorTest.class.getSimpleName()+"_testTableMultIterator_R";
        TestUtil.createTestTable(conn, tableNameR, null);
        itprops.put("R.instanceName",conn.getInstance().getInstanceName());
        itprops.put("R.tableName",tableNameR);
        itprops.put("R.zookeeperHost",conn.getInstance().getZooKeepers());
        //itprops.put("R.timeout","5000");
        itprops.put("R.username",tester.getUsername());
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

        expect = new HashSet<>();
        expect.add(new Pair<>(new Key("A1", "", "B1"), new Value("6".getBytes())));
        expect.add(new Pair<>(new Key("A1", "", "B2"), new Value("12".getBytes())));
        expect.add(new Pair<>(new Key("A2", "", "B2"), new Value("6".getBytes())));

        scanner.close();
        scanner = conn.createScanner(tableNameR, Authorizations.EMPTY);
        {
            Set<Pair<Key, Value>> actual = new HashSet<>();
            log.info("Scan R after compact on Z with TableMultIterator writing to R:");
            for (Map.Entry<Key, Value> entry : scanner) {
                log.info(entry);
                Key k = entry.getKey(); // don't copy vis or timestamp; we don't care about comparing those
                actual.add(new Pair<>(new Key(k.getRow(), k.getColumnFamily(), k.getColumnQualifier()), entry.getValue()));
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
