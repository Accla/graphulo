/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import org.apache.hadoop.io.Text;

import cloudbase.core.client.BatchWriter;
import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Connector;
import cloudbase.core.client.MutationsRejectedException;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Value;
import cloudbase.core.security.ColumnVisibility;

/**
 * @author cyee
 *
 */
public class CbDataLoader {
	String user = null;
	String password = null;
	String instanceName = null;
	String zooKeepers = null;
	String table = null;
	int numEntries = 1;
	int numColEntries= 1;
	String columnFamily="";
	String visibility= "";
	int numThreads= 1;
	long maxMemory = 100000l;
	long maxLatency = 30l;
	static long count=0;


	/**
	 * 
	 */
	public CbDataLoader(String ... args) {
		this.user = args[0];
		this.password = args[1];
		this.instanceName = args[2];
		this.zooKeepers = args[3];
		this.table = args[4];

		this.numEntries = 25000000;
		if(args.length > 5)
			numEntries = Integer.parseInt(args[5]);

		this.numColEntries = 1;
		if(args.length > 6) {
			this.numColEntries = Integer.parseInt(args[6]);
		}
		if(args.length > 7) {
			this.columnFamily = args[7];
		}

		this.visibility = "";


		if (args.length >= 8) {
			numThreads=Integer.parseInt(args[8]);
			System.out.println("NUMBER_OF_THREADS = "+numThreads);
		}
		if(args.length > 9) {
			//ColumnVisibility
			visibility = args[9];
		}
	}

	public void run() {
		System.out.println("user="+user+", password="+password+", instance="+instanceName+
				", table="+table+",zookeeper="+zooKeepers+
				", #threads="+numThreads+", col family="+columnFamily+", number of entries"+numEntries+
				", number of col entries="+numColEntries+", visibility="+visibility);
		ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
		Connector connector = null;
		BatchWriter bw =null;
		long start = System.currentTimeMillis();
		try {
			connector = instance.getConnector(user, password.getBytes());
			if(!connector.tableOperations().exists(table)) {
				connector.tableOperations().create(table);
			}
			bw = connector.createBatchWriter(table, maxMemory, maxLatency, numThreads);
			ColumnVisibility cv = new ColumnVisibility(visibility);

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

		} catch (CBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CBSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		finally {//new Connector(instance, user, pass.getBytes());
			long end = System.currentTimeMillis();
			System.out.println("Total Number of Entries = "+count+" : Time elapsed to write (sec) = "+((end-start)/1000L));
			try {
				bw.close();
			} catch (MutationsRejectedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	public  Mutation createMutation(long rowid, long col, int dataSize, ColumnVisibility visibility, int numEntries){
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

}
