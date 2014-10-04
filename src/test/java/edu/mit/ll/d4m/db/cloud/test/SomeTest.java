package edu.mit.ll.d4m.db.cloud.test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.D4mConfig;
import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations;
import edu.mit.ll.d4m.db.cloud.D4mException;
import edu.mit.ll.d4m.db.cloud.accumulo.AccumuloTableOperations;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.hadoop.io.Text;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Temporary class for testing code.
 */
public class SomeTest {
    private String instanceName = "instance";
    private String zookeeperHost = "localhost";//:2181";
    private String username = "root";
    private String password = "secret";
    private String tableName = "test1";

    private void printList(List<?> list, String prefix) {
        System.out.print(prefix+": ");
        for (Object o : list) {
            System.out.print(o+", ");
        }
        System.out.println();

    }

    @Test
    public void testNormal() throws AccumuloSecurityException, AccumuloException, D4mException, TableNotFoundException, TableExistsException {
        Instance instance = new ZooKeeperInstance(instanceName,zookeeperHost);
        Connector conn = instance.getConnector(username, new PasswordToken(password));
        ConnectionProperties connprops = new ConnectionProperties(zookeeperHost,username,password,instanceName,null);

        innerTest(instance,conn,connprops);
    }


    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void mini() throws AccumuloSecurityException, AccumuloException, D4mException, TableNotFoundException, TableExistsException, IOException, InterruptedException {

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

    private void innerTest(Instance instance, Connector conn, ConnectionProperties connprops) throws AccumuloSecurityException, AccumuloException, D4mException, TableNotFoundException, TableExistsException {
        D4mConfig.getInstance().setCloudType(D4mConfig.ACCUMULO);
        D4mDbTableOperations dbtop = new D4mDbTableOperations(connprops);

        printList(instance.getMasterLocations(), "master_locations");

        if (!conn.tableOperations().exists(tableName))
            conn.tableOperations().create(tableName);

        SortedSet<Text> splitset = new TreeSet<Text>();
        splitset.add(new Text("f"));
        conn.tableOperations().addSplits(tableName, splitset);

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
