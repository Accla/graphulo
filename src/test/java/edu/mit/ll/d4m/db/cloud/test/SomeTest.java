package edu.mit.ll.d4m.db.cloud.test;

import static org.junit.Assert.*;

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
import org.apache.hadoop.io.Text;
import org.junit.Test;

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
	public void test() throws AccumuloSecurityException, AccumuloException, D4mException, TableNotFoundException, TableExistsException {

        Instance instance = new ZooKeeperInstance(instanceName,zookeeperHost);
        Connector conn = instance.getConnector(username, new PasswordToken(password));
        printList(instance.getMasterLocations(), "master_locations");

        if (!conn.tableOperations().exists(tableName))
            conn.tableOperations().create(tableName);

        SortedSet<Text> splitset = new TreeSet<Text>();
        splitset.add(new Text("f"));
        conn.tableOperations().addSplits(tableName, splitset);

        ConnectionProperties connprops = new ConnectionProperties(zookeeperHost,username,password,instanceName,null);
        AccumuloTableOperations ato = new AccumuloTableOperations(connprops);
        List<String> splits = ato.getSplits(tableName, true);
        printList(splits,"splits");

	D4mConfig.getInstance().setCloudType(D4mConfig.ACCUMULO);
	D4mDbTableOperations dbtop = new D4mDbTableOperations(instanceName,zookeeperHost,username,password); //= new D4mDbTableOperations(connprops);
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
