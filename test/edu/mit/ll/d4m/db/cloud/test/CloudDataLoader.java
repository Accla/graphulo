package edu.mit.ll.d4m.db.cloud.test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Properties;

public class CloudDataLoader {
	static String instanceName = "";
	static String host = "";
	static String username = "";
	static String password = "";
	static String table = "";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//Load parameters from a config file
		Properties prop = new Properties();
		ClassLoader cloader = ClassLoader.getSystemClassLoader();
		URL url = cloader.getResource("test_config.properties");

		System.out.println("*****  FILE = "+url.getFile()+"  **********");
		try {
			InputStream ins = url.openStream();
			prop.load(ins);
			ins.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}


		instanceName               = prop.getProperty("cb.instance","cloudbase");
		host                       = prop.getProperty("cb.host","bullet:2181");
		username                   = prop.getProperty("cb.user", "root");
		password                   = prop.getProperty("cb.passwd", "secret");
		table                      = prop.getProperty("cb.table.name", "test_table");
		String columnFamily        = prop.getProperty("cb.col.family", "");
		int numEntries             = Integer.parseInt(prop.getProperty("cb.num.entries", "10"));
		int numColEntries          = Integer.parseInt(prop.getProperty("cb.num.col.entries", "10"));
		long maxMemory = 100000l;
		long maxLatency = 30l;
		int numThreads                    = Integer.parseInt(prop.getProperty("cb.num.threads", "2"));
		String visibility                 = "";
		if(prop.containsKey("cb.visibility"))
			visibility = prop.getProperty("cb.visibility", "");
		String [] params = new String[10];
		params[0] = username;
		params[1] = password;
		params[2] = instanceName;
		params[3] = host;
		params[4] = table;
		params[5] = Integer.toString(numEntries);
		params[6] = Integer.toString(numColEntries);
		params[7] = columnFamily;
		params[8] = Integer.toString(numThreads);
		params[9] = visibility;

		/*
		 * 		String user = args[0];
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

		int numThreads= 1;4.x

		if (args.length >= 8) {
			numThreads=Integer.parseInt(args[8]);
			System.out.println("NUMBER_OF_THREADS = "+numThreads);
		}
		if(args.length > 9) {
			//ColumnVisibility
			visibility = args[9];
		}

		 */
		try {
			Class <?>  runtmp=null;
			//		ZooKeeperInstance instance = new ZooKeeperInstance(instanceName, zooKeepers);
			//		Connector connector;
			//		try {
			//			connector = instance.getConnector(user, pass.getBytes());
			//
			//			if(!connector.tableOperations().exists(table)) {
			//				connector.tableOperations().create(table);
			//			}
			//			BatchWriter bw = connector.createBatchWriter(table, maxMemory, maxLatency, numThreads);
			//ClassLoader cloader = Thread.currentThread().getContextClassLoader();
			Class<?> cls = cloader.loadClass("edu.mit.ll.d4m.db.cloud.TestBatchWriter");
			Method main = null;
			main = cls.getMethod("main", params.getClass());
			final Object theArgs= params;
			final Method finalMain = main;
			Runnable r=null;
			r = new Runnable() {
				public void run() {
					try {
						finalMain.invoke(null, theArgs);
					} catch( Exception e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
				}
			};

			Thread t = new Thread(r,"MyBatchWriter");
			t.start();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 


	}

}
