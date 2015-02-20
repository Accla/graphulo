package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Test DotRemoteSourceIterator.
 */
public class DotRemoteIteratorTest {
    private static final Logger log = LogManager.getLogger(DotRemoteIteratorTest.class);

    /** This is setup once for the entire class. */
    @ClassRule
    public static IAccumuloTester tester = ACCUMULO_TEST_CONFIG.AccumuloTester;

    @Test
    public void testSortedMapComparator() {
        Key k1 = new Key("row1","colF1","colQ1");
        Key k2 = new Key("row2","colF1","colQ1");
        Key k3 = new Key("row3","colF1","colQ1");

        SortedMap<Key,Integer> map = new TreeMap<>(new DotRemoteSourceIterator.ColFamilyQualifierComparator());
        map.put(k1, 1);
        map.put(k2, 2);
        int v = map.get(k3);
        Assert.assertEquals(2,v);
        //log.info("map returned: "+v);
    }

    /**
     * <pre>
     *      C1 C2        C1 C2 C3          B1  B2
     * A1 [ 2  2 ] * B1 [   3  3  ] = A1 [ 6   6,6 ]
     * A2 [ 2    ]   B2 [3  3     ]   A2 [     6   ]
     * </pre>
     */
    @Test
    public void testSource() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        final String tableNameA = "test_"+DotRemoteIteratorTest.class.getSimpleName()+"_testSource_A";
        Connector conn = tester.getConnector();

        if (conn.tableOperations().exists(tableNameA)) {
            conn.tableOperations().delete(tableNameA);
        }
        conn.tableOperations().create(tableNameA);

        // write some values to tableName
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(10000000L); // bytes available to batchwriter for buffering mutations
        BatchWriter writer = conn.createBatchWriter(tableNameA,config);
        {
            Text[] rows = new Text[]{new Text("A1"), new Text("A1"), new Text("A2")};
            Text cf = new Text("");
            Text[] cqs = new Text[] { new Text("C1"), new Text("C2"), new Text("C1")} ;
            Value v = new Value("2".getBytes());
            for (int i = 0; i < rows.length; i++) {
                Mutation m = new Mutation(rows[i]);
                m.put(cf, cqs[i], v);
                writer.addMutation(m);
            }
            writer.flush();
        }
        writer.close();

        final String tableNameBT = "test_"+DotRemoteIteratorTest.class.getSimpleName()+"_testSource_BT";
        if (conn.tableOperations().exists(tableNameBT)) {
            conn.tableOperations().delete(tableNameBT);
        }
        conn.tableOperations().create(tableNameBT);
        writer = conn.createBatchWriter(tableNameBT, config);
        {
            Text[] rows = new Text[]{new Text("B1"), new Text("B1"), new Text("B2"), new Text("B2")};
            Text cf = new Text("");
            Text[] cqs = new Text[] { new Text("C2"), new Text("C3"), new Text("C1"), new Text("C2")} ;
            Value v = new Value("3".getBytes());
            for (int i = 0; i < rows.length; i++) {
                Mutation m = new Mutation(rows[i]);
                m.put(cf, cqs[i], v);
                writer.addMutation(m);
            }
            writer.flush();
        }
        writer.close();

        final String tableNameC = "test_"+DotRemoteIteratorTest.class.getSimpleName()+"_testSource_C";
        if (conn.tableOperations().exists(tableNameC)) {
            conn.tableOperations().delete(tableNameC);
        }
        conn.tableOperations().create(tableNameC);

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
        IteratorSetting itset = new IteratorSetting(25, DotRemoteSourceIterator.class, itprops);
        scanner.addScanIterator(itset);
        log.info("Results of scan on table " + tableNameC + " with A=" + tableNameA + " and BT="+tableNameBT+':');
        for (Map.Entry<Key, Value> entry : scanner) {
            log.info(entry);
            //log.info("decoded: " + WholeRowIterator.decodeRow(entry.getKey(), entry.getValue())); // *
        }

        conn.tableOperations().delete(tableNameA);
        conn.tableOperations().delete(tableNameBT);
        conn.tableOperations().delete(tableNameC);
    }



}
