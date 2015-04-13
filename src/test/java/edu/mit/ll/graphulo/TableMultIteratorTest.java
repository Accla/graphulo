package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
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

    @Test
    public void testPeekingIterator2() {
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        Iterator<Integer> iFirst = list.iterator(), iSecond = list.iterator();
        iSecond.next();
        PeekingIterator2<Integer> pe = new PeekingIterator2<>(list.iterator());
        while (pe.hasNext()) {
            Assert.assertTrue(iFirst.hasNext());
            Assert.assertEquals(iFirst.next(), pe.peekFirst());
            if (iSecond.hasNext())
                Assert.assertEquals(iSecond.next(), pe.peekSecond());
            else
                Assert.assertNull(pe.peekSecond());
            pe.next();
        }
        Assert.assertNull(pe.peekFirst());
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
        itprops.put("AT.instanceName",conn.getInstance().getInstanceName());
        itprops.put("AT.tableName",tableNameA);
        itprops.put("AT.zookeeperHost",conn.getInstance().getZooKeepers());
        itprops.put("AT.username",tester.getUsername());
        itprops.put("AT.password",new String(tester.getPassword().getPassword()));
        itprops.put("B.instanceName",conn.getInstance().getInstanceName());
        itprops.put("B.tableName",tableNameBT);
        itprops.put("B.zookeeperHost",conn.getInstance().getZooKeepers());
        itprops.put("B.username",tester.getUsername());
        itprops.put("B.password",new String(tester.getPassword().getPassword()));
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
//    @Ignore("New version only works with BatchWriter")
    @Test
    public void testTableMultIterator() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        Connector conn = tester.getConnector();
        final String tablePrefix = "testTableMult_";

        final String tableNameAT = tablePrefix+"AT";
        {
            Map<Key, Value> input = new HashMap<>();
            input.put(new Key("A1", "", "C1"), new Value("2".getBytes()));
            input.put(new Key("A1", "", "C2"), new Value("2".getBytes()));
            input.put(new Key("A2", "", "C1"), new Value("2".getBytes()));
            input = TestUtil.tranposeMap(input);
            TestUtil.createTestTable(conn, tableNameAT, null, input);
        }
        SortedSet<Text> splitSet = new TreeSet<>();
        splitSet.add(new Text("A15"));
        //conn.tableOperations().addSplits(tableNameAT, splitSet);

        final String tableNameB = tablePrefix+"B";
        {
            Map<Key, Value> input = new HashMap<>();
            input.put(new Key("B1", "", "C2"), new Value("3".getBytes()));
            input.put(new Key("B1", "", "C3"), new Value("3".getBytes()));
            input.put(new Key("B2", "", "C1"), new Value("3".getBytes()));
            input.put(new Key("B2", "", "C2"), new Value("3".getBytes()));
            input = TestUtil.tranposeMap(input);
            TestUtil.createTestTable(conn, tableNameB, null, input);
        }

        final String tableNameC = tablePrefix+"C";
        {
            Map<Key, Value> input = new HashMap<>();
            input.put(new Key("A1", "", "B1"), new Value("1".getBytes()));
            TestUtil.createTestTable(conn, tableNameC, splitSet, input);
            Map<String, String> optSum = new HashMap<>();
            optSum.put("all", "true");
            conn.tableOperations().attachIterator(tableNameC,
                new IteratorSetting(1,BigDecimalCombiner.BigDecimalSummingCombiner.class,optSum));
        }

        BatchScanner scannerB = conn.createBatchScanner(tableNameB, Authorizations.EMPTY, 2);
        scannerB.setRanges(Collections.singleton(new Range()));

        // test reading entries directly

        {
            Map<String,String> itprops = new HashMap<>();
            itprops.put("AT.instanceName",conn.getInstance().getInstanceName());
            itprops.put("AT.tableName",tableNameAT);
            itprops.put("AT.zookeeperHost",conn.getInstance().getZooKeepers());
            itprops.put("AT.username",tester.getUsername());
            itprops.put("AT.password",new String(tester.getPassword().getPassword()));
//            itprops.put("B.instanceName",conn.getInstance().getInstanceName());
//            itprops.put("B.tableName",tableNameB);
//            itprops.put("B.zookeeperHost",conn.getInstance().getZooKeepers());
//            itprops.put("B.username",tester.getUsername());
//            itprops.put("B.password",new String(tester.getPassword().getPassword()));
            IteratorSetting itset = new IteratorSetting(15, TableMultIteratorQuery.class, itprops);
            scannerB.addScanIterator(itset);

            Map<Key, Integer> expect = new HashMap<>();
            expect.put(new Key("A1", "", "B1"), 6);
            expect.put(new Key("A1", "", "B2"), 12);
            expect.put(new Key("A2", "", "B2"), 6);
            //scannerB.addScanIterator(new IteratorSetting(16, DebugInfoIterator.class, Collections.<String,String>emptyMap()));
            {
                Map<Key, Integer> actual = new HashMap<>();
                log.info("Scanning tableB " + tableNameB + ":");
                for (Map.Entry<Key, Value> entry : scannerB) {
                    log.info(entry);
                    Key k = entry.getKey(); // don't copy vis or timestamp; we don't care about comparing those
                    Key k2 = new Key(k.getRow(), k.getColumnFamily(), k.getColumnQualifier());
                    Integer vold = actual.get(k2);
                    if (vold == null)
                        vold = 0;
                    int vnew = vold + Integer.parseInt(entry.getValue().toString());
                    actual.put(k2, vnew);
                }
                Assert.assertEquals(expect, actual);
            }
        }



        // test writing to C with monitoring
        {
            Map<String,String> itprops = new HashMap<>();
            itprops.put("AT.instanceName",conn.getInstance().getInstanceName());
            itprops.put("AT.tableName",tableNameAT);
            itprops.put("AT.zookeeperHost",conn.getInstance().getZooKeepers());
            itprops.put("AT.username",tester.getUsername());
            itprops.put("AT.password",new String(tester.getPassword().getPassword()));
            itprops.put("C.instanceName",conn.getInstance().getInstanceName());
            itprops.put("C.tableName",tableNameC);
            itprops.put("C.zookeeperHost",conn.getInstance().getZooKeepers());
            itprops.put("C.username",tester.getUsername());
            itprops.put("C.password",new String(tester.getPassword().getPassword()));
            itprops.put("C.numEntriesCheckpoint", "1");
            IteratorSetting itset = new IteratorSetting(15, TableMultIteratorQuery.class, itprops);
            scannerB.clearScanIterators();
            scannerB.addScanIterator(itset);
            for (Map.Entry<Key, Value> entry : scannerB) {
                ;
            }
            scannerB.close();

            Map<Key, Value> expect = new HashMap<>();
            expect.put(new Key("A1", "", "B1"), new Value("7".getBytes()));
            expect.put(new Key("A1", "", "B2"), new Value("12".getBytes()));
            expect.put(new Key("A2", "", "B2"), new Value("6".getBytes()));

            Scanner scannerC = conn.createScanner(tableNameC, Authorizations.EMPTY);
            //scannerC.addScanIterator(new IteratorSetting(16, DebugInfoIterator.class, Collections.<String,String>emptyMap()));
            {
                Map<Key, Value> actual = new HashMap<>();
                log.info("Scanning tableC " + tableNameC + ":");
                for (Map.Entry<Key, Value> entry : scannerC) {
                    log.info(entry);
                    Key k = entry.getKey(); // don't copy vis or timestamp; we don't care about comparing those
                    actual.put(new Key(k.getRow(), k.getColumnFamily(), k.getColumnQualifier()), entry.getValue());
                }
                Assert.assertEquals(expect, actual);
            }
            scannerC.close();
        }

        conn.tableOperations().delete(tableNameAT);
        conn.tableOperations().delete(tableNameB);
        conn.tableOperations().delete(tableNameC);
    }

}
