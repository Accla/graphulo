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
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Test RemoteIterator.
 */
public class RemoteIteratorTest {
    private static final Logger log = LogManager.getLogger(RemoteIteratorTest.class);

    /** This is setup once for the entire class. */
    @ClassRule
    public static IAccumuloTester tester = ACCUMULO_TEST_CONFIG.AccumuloTester;

    @Test
    public void test1() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        final String tableName = "test_"+RemoteIteratorTest.class.getSimpleName()+"_test1";
        Connector conn = tester.getConnector();

        if (conn.tableOperations().exists(tableName)) {
            conn.tableOperations().delete(tableName);
        }
        conn.tableOperations().create(tableName);

        // write some values to tableName
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(10000000L); // bytes available to batchwriter for buffering mutations
        BatchWriter writer = conn.createBatchWriter(tableName,config);
        {
            Text[] rows = new Text[]{new Text("ccc"), new Text("ddd"), new Text("pogo")};
            Text cf = new Text("");
            Text cq = new Text("cq");
            Value v = new Value("7".getBytes());
            for (Text row : rows) {
                Mutation m = new Mutation(row);
                m.put(cf, cq, v);
                writer.addMutation(m);
            }
            writer.flush();
        }
        {
            Text[] rows = new Text[]{new Text("ddd"), new Text("ggg"), new Text("pogo"), new Text("xyz")};
            Text cf = new Text("");
            Text cq = new Text("cq2");
            Value v = new Value("8".getBytes());
            for (Text row : rows) {
                Mutation m = new Mutation(row);
                m.put(cf, cq, v);
                writer.addMutation(m);
            }
            writer.flush();
        }

        final String tableName2 = "test_"+RemoteIteratorTest.class.getSimpleName()+"_test1_2";
        if (conn.tableOperations().exists(tableName2)) {
            conn.tableOperations().delete(tableName2);
        }
        conn.tableOperations().create(tableName2);

        Scanner scanner = conn.createScanner(tableName2, Authorizations.EMPTY);
        Map<String,String> itprops = new HashMap<>();
        itprops.put("instanceName",conn.getInstance().getInstanceName());
        itprops.put("tableName",tableName);
        itprops.put("zookeeperHost",conn.getInstance().getZooKeepers());
        itprops.put("timeout","5000");
        itprops.put("username",tester.getUsername());
        itprops.put("password",new String(tester.getPassword().getPassword()));
        itprops.put("doWholeRow","true"); // *
        IteratorSetting itset = new IteratorSetting(5, "RemoteIterator", RemoteIterator.class, itprops); //"edu.mit.ll.graphulo.RemoteIterator", itprops);
        scanner.addScanIterator(itset);
        System.out.println("Results of scan on table "+tableName2+" remote to "+tableName+':');
        for (Map.Entry<Key, Value> entry : scanner) {
            System.out.println(entry);
            System.out.println("decoded: "+ WholeRowIterator.decodeRow(entry.getKey(), entry.getValue())); // *
        }


    }

}
