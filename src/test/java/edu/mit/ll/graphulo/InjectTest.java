package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests to inject entries into the Accumulo iterator stream.
 */
public class InjectTest {
    private static final Logger log = LogManager.getLogger(InjectTest.class);

    /** This is setup once for the entire class. */
    @ClassRule
    public static IAccumuloTester tester = ACCUMULO_TEST_CONFIG.AccumuloTester;

    private void logList(Collection<?> list, String prefix) {
        StringBuilder sb = new StringBuilder(prefix+": ");
        for (Object o : list) {
            sb.append(o.toString()).append(',');
        }
        log.debug(sb.substring(0, sb.length() - 1));
    }

    @Test
    public void testInsertScan() throws Exception {
        final String tableName = "test_"+InjectTest.class.getSimpleName()+"_testInsertScan";
        Connector conn = tester.getConnector();
        if (conn.tableOperations().exists(tableName)) {
            conn.tableOperations().delete(tableName);
        }
        conn.tableOperations().create(tableName);

        // make a table split in tableName
        SortedSet<Text> splitset = new TreeSet<Text>();
        splitset.add(new Text("f"));
        conn.tableOperations().addSplits(tableName, splitset);

        // write some values to tableName
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(10000000L); // bytes available to batchwriter for buffering mutations
        BatchWriter writer = conn.createBatchWriter(tableName,config);
        Text[] rows = new Text[] {new Text("ccc"), new Text("ddd"), new Text("pogo")};
        Text cf = new Text("");
        Text cq = new Text("cq");
        Value v = new Value("7".getBytes());
        for (Text row : rows) {
            Mutation m = new Mutation(row);
            m.put(cf, cq, v);
            writer.addMutation(m);
        }
        writer.flush();

        // read back
        Scanner scan = conn.createScanner(tableName, Authorizations.EMPTY);
        Key startKey = new Key(new Text("d"), cf, cq);
        Range rng = new Range(startKey,null,true,false,false,true);
        scan.setRange(rng);// (new Key("d"));
        log.debug("Results of scan on table "+tableName+':');
        Iterator<Map.Entry<Key, Value>> iterator = scan.iterator();

        Key k2 = new Key(new Text("ddd"), cf, cq);
        Key k3 = new Key(new Text("pogo"), cf, cq);
        Map.Entry<Key, Value> r1 = iterator.next();
        assertTrue(k2.equals(r1.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
        assertEquals(v,r1.getValue());
        Map.Entry<Key, Value> r2 = iterator.next();
        assertTrue(k3.equals(r2.getKey(),PartialKey.ROW_COLFAM_COLQUAL_COLVIS));
        assertEquals(v,r2.getValue());
        assertFalse(iterator.hasNext());

        for (Map.Entry<Key, Value> kv : scan) {
            log.debug(kv);
        }

        // delete test data
        conn.tableOperations().delete(tableName);

    }

    @Test
    public void testInjectOnScan() throws Exception {
        final String tableName = "test_"+InjectTest.class.getSimpleName()+"_testInjectOnScan";
        Connector conn = tester.getConnector();
        if (conn.tableOperations().exists(tableName)) {
            conn.tableOperations().delete(tableName);
        }
        conn.tableOperations().create(tableName);

        // make a table split in tableName
        SortedSet<Text> splitset = new TreeSet<Text>();
        splitset.add(new Text("row3"));
        conn.tableOperations().addSplits(tableName, splitset);

        // attach InjectIterator
        IteratorSetting itset = new IteratorSetting(15, InjectIterator.class);
        conn.tableOperations().attachIterator(tableName, itset, EnumSet.of(IteratorUtil.IteratorScope.scan));

        // add a couple entries
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(10000000L); // bytes available to batchwriter for buffering mutations
        BatchWriter writer = conn.createBatchWriter(tableName,config);
        Text[] rows = new Text[] {new Text("row2"), new Text("row4"), new Text("row8")};
        Text cf = new Text("colF3");
        Text cq = new Text("colQ3");
        Value v = new Value("7".getBytes());
        for (Text row : rows) {
            Mutation m = new Mutation(row);
            m.put(cf, cq, v);
            writer.addMutation(m);
        }
        writer.flush();

        // get the split information
        logList(conn.tableOperations().listSplits(tableName), tableName+" table splits");

        // scan table
        Scanner scan = conn.createScanner(tableName, Authorizations.EMPTY);

        // get the split information
        logList(conn.tableOperations().listSplits(tableName), tableName+" table splits");

        log.debug("Results of scan on table "+tableName+':');
        for (Map.Entry<Key, Value> kv : scan) {
            log.debug(kv);
        }

        // delete test data
        conn.tableOperations().delete(tableName);
    }

    @Test
    public void testInjectOnCompact() throws Exception {
        final String tableName = "test_"+InjectTest.class.getSimpleName()+"_testInjectOnCompact";
        Connector conn = tester.getConnector();
        if (conn.tableOperations().exists(tableName)) {
            conn.tableOperations().delete(tableName);
        }
        conn.tableOperations().create(tableName);

        // make a table split in tableName
        SortedSet<Text> splitset = new TreeSet<Text>();
        splitset.add(new Text("row3"));
        conn.tableOperations().addSplits(tableName, splitset);

        // add a couple entries
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(10000000L); // bytes available to batchwriter for buffering mutations
        BatchWriter writer = conn.createBatchWriter(tableName,config);
        Text[] rows = new Text[] {new Text("row2"), new Text("row4"), new Text("row8")};
        Text cf = new Text("colF3");
        Text cq = new Text("colQ3");
        Value v = new Value("7".getBytes());
        for (Text row : rows) {
            Mutation m = new Mutation(row);
            m.put(cf, cq, v);
            writer.addMutation(m);
        }
        writer.flush();

        // get the split information
        logList(conn.tableOperations().listSplits(tableName), tableName+" table splits");

        // attach InjectIterator, flush and compact
        // compaction will block.
        IteratorSetting itset = new IteratorSetting(15, InjectIterator.class);
        StopWatch sw = new StopWatch();
        sw.start();
        conn.tableOperations().compact(tableName, null, null, Collections.singletonList(itset), true, true);
        sw.stop();
        log.debug("compaction took "+sw.getTime()+" ms");

        // get the split information
        logList(conn.tableOperations().listSplits(tableName), tableName+" table splits");


        // scan table
        Scanner scan = conn.createScanner(tableName, Authorizations.EMPTY);

        log.debug("Results of scan on table "+tableName+':');
        for (Map.Entry<Key, Value> kv : scan) {
            log.debug(kv);
        }

        // delete test data
        conn.tableOperations().delete(tableName);
    }
}
