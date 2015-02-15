package testing;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Example tests.
 */
public class ATest extends MiniAccumuloTester {
    private static final Logger log = LogManager.getLogger(ATest.class);

    /** This is setup once for the entire class. */
    @ClassRule
    public static IAccumuloTester tester = ACCUMULO_TEST_CONFIG.AccumuloTester;

    @Test
    public void testInsertScan() throws Exception {
        final String tableName = "test_"+ATest.class.getSimpleName();
        Connector conn = tester.getConnector();
        // create tableName
        if (!conn.tableOperations().exists(tableName))
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
        for (int i=0; i<rows.length; i++) {
            Mutation m = new Mutation(rows[i]);
            m.put(cf, cq, v);
            writer.addMutation(m);
        }
        writer.flush();

        // read back
        Scanner scan = conn.createScanner(tableName, Authorizations.EMPTY);
        Key startKey = new Key(new Text("d"), cf, cq);
        Range rng = new Range(startKey,null,true,false,false,true);
        scan.setRange(rng);// (new Key("d"));
        log.debug(tableName+" table:");
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
}
