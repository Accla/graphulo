package edu.mit.ll.d4m.db.cloud;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.hadoop.io.Text;

import cloudbase.core.conf.CBConfiguration;
import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Connector;
import cloudbase.core.client.TableExistsException;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.client.impl.MasterClient;
import cloudbase.core.client.impl.Tables;
import cloudbase.core.client.impl.ThriftTransportPool;
import cloudbase.core.master.thrift.MasterClientService;
import cloudbase.core.master.thrift.MasterMonitorInfo;
import cloudbase.core.master.thrift.TabletInfo;
import cloudbase.core.master.thrift.TabletServerStatus;
import cloudbase.core.security.thrift.AuthInfo;
import cloudbase.core.tabletserver.thrift.TabletClientService;
import cloudbase.core.util.AddressUtil;
import cloudbase.core.security.thrift.AuthInfo;
import cloudbase.core.master.thrift.TableInfo;
import cloudbase.core.client.admin.TableOperations;
import cloudbase.core.client.BatchWriter;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Value;
import cloudbase.core.security.ColumnVisibility;

public class TestBatchWriter {

	static String columnFamily="";
	static long count=0;
	
	
	public void run() {
		
	}
	/**
	 * @param args
	 * @throws CBSecurityException 
	 * @throws CBException 
	 * @throws TableExistsException 
	 * @throws TableNotFoundException 
	 */
	public static void main(String[] args) throws CBException, CBSecurityException, TableExistsException, TableNotFoundException {
		// TODO Auto-generated method stub
		String user = args[0];
		String pass = args[1];
		String instanceName = args[2];
		String zooKeepers = args[3];
		String table = args[4];

		int numEntries = 25000000;
		if(args.length > 5)
			numEntries = Integer.parseInt(args[5]);

		int numColEntries = 1;
		if(args.length > 6) {
			numColEntries = Integer.parseInt(args[6]);
		}
		if(args.length > 7) {
			columnFamily = args[7];
		}
		
		String visibility = "";

		int numThreads= 1;
		if (args.length >= 8) {
			numThreads=Integer.parseInt(args[8]);
			System.out.println("NUMBER_OF_THREADS = "+numThreads);
		}
		if(args.length > 9) {
			//ColumnVisibility
			visibility = args[9];
		}
		long maxMemory = 100000l;
		long maxLatency = 30l;
		

		System.out.println("user="+user+", password="+pass+", instance="+instanceName+
				", table="+table+",zookeeper="+zooKeepers+
				", #threads="+numThreads+", col family="+columnFamily+", number of entries"+numEntries+
				", number of col entries="+numColEntries+", visibility="+visibility);
		ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
		Connector connector = instance.getConnector(user, pass.getBytes());//new Connector(instance, user, pass.getBytes());
		if(!connector.tableOperations().exists(table)) {
			connector.tableOperations().create(table);
		}
		BatchWriter bw = connector.createBatchWriter(table, maxMemory, maxLatency, numThreads);
		ColumnVisibility cv = new ColumnVisibility(visibility);
		long start = System.currentTimeMillis();
		for(int i = 0; i < numEntries; i++) {
			long stime= System.currentTimeMillis();
			long rowid = i;
			//for (int j = 0; j < numColEntries; j++) {
				long colQual = count; //(long)j;
				Mutation mut = createMutation(rowid, colQual, 256,cv, numColEntries);

				bw.addMutation(mut);
				count++;
			//}
			//System.out.println(i+ " :: elapsed time (msec) = "+((System.currentTimeMillis()-stime)));
			//Thread.sleep(500l);
		}
		long end = System.currentTimeMillis();
		System.out.println("Total Number of Entries = "+count+" : Time elapsed to write (sec) = "+((end-start)/1000L));
		bw.close();

	}
	public static Mutation createMutation(long rowid, int dataSize, ColumnVisibility visibility){
		Text row = new Text(String.format("row_%010d", rowid));

		Mutation m = null;//new Mutation(row);
		Random rand = new Random(rowid);
		//create a random value that is a function of the
		//row id for verification purposes
		long time=System.currentTimeMillis()+1L;
		byte value[] = Long.toString(time).getBytes();//createValue(rowid, dataSize);
		m = createMutation(rowid,time,dataSize,visibility);
		//m.put(new Text("boo"), new Text("foo"+Long.toString(time)), visibility, new Value(value));
		return m;
	}
	public static Mutation createMutation(long rowid, long col, int dataSize, ColumnVisibility visibility){
		Text row = new Text(String.format("row_%010d", rowid));

		Mutation m = new Mutation(row);
		Random rand = new Random(rowid);
		//create a random value that is a function of the
		//row id for verification purposes
		long time=col;//System.currentTimeMillis()+1L;
		byte value[] = Long.toString(time).getBytes();//createValue(rowid, dataSize);

		// Mutation.put(columnFamily, columnQualifier, visibility, value)
		m.put(new Text(columnFamily.trim()), new Text(columnFamily+"_foo"+Long.toString(time)), visibility, new Value(value));

		return m;
	}
	public static Mutation createMutation(long rowid, long col, int dataSize, ColumnVisibility visibility, int numEntries){
		Text row = new Text(String.format("row_%010d", rowid));

		Mutation m = new Mutation(row);
		for(int i=0; i < numEntries; i++) {
			//create a random value that is a function of the
			//row id for verification purposes
			long time=col+(long)i;//System.currentTimeMillis()+1L;
			byte value[] = Long.toString(time).getBytes();//createValue(rowid, dataSize);

			// Mutation.put(columnFamily, columnQualifier, visibility, value)
			m.put(new Text(columnFamily.trim()), new Text(columnFamily+"_foo"+Long.toString(time)), visibility, new Value(value));
		}
		return m;
	}

	public static byte[] createValue(long rowid, int dataSize){
		Random r = new Random(rowid);
		byte value[] = new byte[dataSize];

		r.nextBytes(value);

		//transform to printable chars
		for(int j = 0; j < value.length; j++) {
			value[j] = (byte)(( (0xff & value[j]) % 92) + ' ');
		}

		return value;
	}

}
