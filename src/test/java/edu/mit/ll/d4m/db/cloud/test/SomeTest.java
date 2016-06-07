package edu.mit.ll.d4m.db.cloud.test;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations;
import edu.mit.ll.d4m.db.cloud.accumulo.AccumuloTableOperations;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Temporary class for testing code.
 */
public class SomeTest {
    private String instanceName = "instance-1.7.0";
    private String zookeeperHost = "localhost";//:2181";
    private String username = "root";
    private String password = "secret";
    private String tableName = "test1";

    private static ClientConfiguration txe1config;
    static {
        String instance = "classdb51";
        String host = "classdb51.cloud.llgrid.txe1.mit.edu:2181";
        int timeout = 10000;
        txe1config = ClientConfiguration.loadDefault().withInstance(instance).withZkHosts(host).withZkTimeout(timeout);
    }

    private void printList(Collection<?> list, String prefix) {
        System.out.print(prefix+": ");
        for (Object o : list) {
            System.out.print(o+", ");
        }
        System.out.println();
    }

    static String[] getTXE1UserPass() throws IOException {
        String user = "AccumuloUser";
        BufferedReader f = new BufferedReader(new FileReader("clouddb51_pass.txt"));
        String pass = f.readLine();
        f.close();
        return new String[] { user, pass};
    }

    @Ignore
    @Test
    public void testTXE1() throws Exception {
        Assume.assumeTrue("Test requires TXE1", new File("clouddb51_pass.txt").exists());

        Instance instance = new ZooKeeperInstance(txe1config.get(ClientConfiguration.ClientProperty.INSTANCE_NAME), txe1config.get(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST));
        String[] tmp = getTXE1UserPass();
        String user = tmp[0];
        String pass = tmp[1];
        Connector conn = instance.getConnector(user, new PasswordToken(pass));
        ConnectionProperties connprops = new ConnectionProperties(txe1config.get(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST),user,pass,txe1config.get(ClientConfiguration.ClientProperty.INSTANCE_NAME),null);

        innerTest(instance,conn,connprops);
    }

    @Ignore
    @Test
    public void testlocal() throws Exception {
        String instanceName = "Dev";
        String host = "localhost:2181";
        int timeout = 10000;
        ClientConfiguration local = ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(host).withZkTimeout(timeout);
        Instance instance = new ZooKeeperInstance(local.get(ClientConfiguration.ClientProperty.INSTANCE_NAME), local.get(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST));
        String user = "root";
        Connector conn = instance.getConnector(user, new PasswordToken("secret"));
        ConnectionProperties connprops = new ConnectionProperties(local.get(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST),user,"secret",local.get(ClientConfiguration.ClientProperty.INSTANCE_NAME),null);

        innerTest(instance,conn,connprops);
    }

    @Ignore
    @Test
    public void testNormal() throws Exception {
        Instance instance = new ZooKeeperInstance(instanceName,zookeeperHost);
        Connector conn = instance.getConnector(username, new PasswordToken(password));
        ConnectionProperties connprops = new ConnectionProperties(zookeeperHost,username,password,instanceName,null);

        innerTest(instance,conn,connprops);
    }


    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    static {
        org.apache.accumulo.core.client.ClientConfiguration.loadDefault();
    }

    @Test
    public void mini() throws Exception {

        File tempDir = tempFolder.newFolder();
        MiniAccumuloCluster accumulo = new MiniAccumuloCluster(tempDir, "password");
        accumulo.start(); // doesn't work on Dylan's computer for some reason.  The OS closes the Zookeeper connection.
        Instance instance = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
        Connector conn = instance.getConnector("root", new PasswordToken("password"));
        ConnectionProperties connprops = new ConnectionProperties(accumulo.getZooKeepers(),"root","password",accumulo.getInstanceName(),null);

        innerTest(instance,conn,connprops);

        accumulo.stop();
        tempDir.delete();
    }

    private void innerTest(Instance instance, Connector conn, ConnectionProperties connprops) throws Exception {
        D4mDbTableOperations dbtop = new D4mDbTableOperations(connprops);

        printList(conn.tableOperations().list(), "tables");
        printList(instance.getMasterLocations(), "master_locations");

	// create tableName
        if (!conn.tableOperations().exists(tableName))
            conn.tableOperations().create(tableName);

	// make a table split in tableName
        SortedSet<Text> splitset = new TreeSet<>();
        splitset.add(new Text("f"));
        conn.tableOperations().addSplits(tableName, splitset);


	// write some values to tableName
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(10000000L); // bytes available to batchwriter for buffering mutations
        BatchWriter writer = conn.createBatchWriter(tableName,config);
        Text[] rows = new Text[] {new Text("ccc"), new Text("ddd"), new Text("pogo")};
        Text cf = new Text("");
        Text cq = new Text("cq");
        Value v = new Value("7".getBytes(StandardCharsets.UTF_8));
        for (Text row : rows) {
            Mutation m = new Mutation(row);
            m.put(cf, cq, v);
            writer.addMutation(m);
        }
        writer.flush();

	// read from Metadata table
        Scanner scan = conn.createScanner(AccumuloTableOperations.METADATA_TABLE_NAME, Authorizations.EMPTY);
        System.out.println(AccumuloTableOperations.METADATA_TABLE_NAME+" table:");
        for (Map.Entry<Key, Value> kv : scan) {
            System.out.println(kv);
        }

	// get the split information
        AccumuloTableOperations ato = new AccumuloTableOperations(connprops);
        List<String> splits = ato.getSplits(tableName, true);
        printList(splits,"splits");

        String[] infos = dbtop.getAllSplitsInfo(tableName);
        System.out.println("SplitNames: "+infos[0]);
        System.out.println("SplitNums : "+infos[1]);
        System.out.println("SplitTSs  : "+infos[2]);

        /*
          Running edu.mit.ll.d4m.db.cloud.test.SomeTest
          master_locations: 10.211.55.100:9999,
          TabletStat name:2
          Expected   name:2
          TabletStat name:2
          Expected   name:2
          TabletStat name:2
          Expected   name:2
          splits: f, :, 0, 3,
          TabletStat name:2
          Expected   name:2
          TabletStat name:2
          Expected   name:2
          TabletStat name:2
          Expected   name:2
          SplitNames: f,
          SplitNums : 0,3,
          SplitTSs  : 10.211.55.101:9997,
         */
    }


}
