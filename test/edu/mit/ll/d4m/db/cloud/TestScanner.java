package edu.mit.ll.d4m.db.cloud;

import java.util.HashSet;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;

import cloudbase.core.CBConstants;
import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Connector;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;

/*
 *  Test program to see how the BatchScanner works and how to set up query.
 */
public class TestScanner {

    public static void main(String[] args) throws CBException, CBSecurityException, TableNotFoundException
    {
		String instanceName = args[0];
		String zooKeepers = args[1];
		String user = args[2];
		byte[] pass = args[3].getBytes();
		String table = args[4];
		int num = 1;
		int numThreads = Integer.parseInt(args[5]);
		String rowQuery = "GOO10000";
		if(args.length == 7 )
		    rowQuery = args[6];

		
		ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
		Connector connector = new Connector(instance, user, pass);
		BatchScanner tsbr = connector.createBatchScanner(table, CBConstants.NO_AUTHS, numThreads);

		HashSet<Range> ranges = new HashSet<Range>(num);

		//Set up the query
		//place query in Range object,
		// and place Range object in the HashSet 
		Text row1 = new Text(rowQuery);
		Range range = new Range(new Text(row1));
		ranges.add(range);

		//Set HashSet in scanner, and it auto-magically gets back the results.
		tsbr.setRanges(ranges);
		System.out.println("\n");
		for (Entry<Key,Value> entry : tsbr) {
		    System.out.println("KEY = "+entry.getKey()+ ", VALUE="+ entry.getValue());
		    System.out.println("ROW = "+entry.getKey().getRow().toString()+ ", VALUE=" + entry.getValue());
		}


    }
}
