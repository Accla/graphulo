/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import org.apache.log4j.Logger;

import edu.mit.ll.cloud.connection.ConnectionProperties;

/**
 * Factory class to create CloudbaseQuery or AccumuloQuery objects
 * @author CHV8091
 *
 */
public class D4mFactory {
	private static Logger log = Logger.getLogger(D4mFactory.class);

	public static String PROPERTY_D4M_CLOUD_TYPE="d4m.cloud.type";
	public static String CLOUD_TYPE="BigTableLike"; // BigTableLike or Accumulo

	public static String CLOUDBASE_QUERY="edu.mit.ll.d4m.db.cloud.CloudbaseQuery";
	public static String ACCUMULO_QUERY="edu.mit.ll.d4m.db.cloud.D4mAccumQuery";
	public static String CLOUDBASE_QUERY2="edu.mit.ll.d4m.db.cloud.D4mDbQueryCloudbase";
	public static String ACCUMULO_QUERY2="edu.mit.ll.d4m.db.cloud.D4mDbQueryAccumulo";

	public static String CLOUDBASE_INSERT="edu.mit.ll.d4m.db.cloud.CloudbaseInsert";
	public static String ACCUMULO_INSERT="edu.mit.ll.d4m.db.cloud.AccumuloInsert";
	public static String CLOUDBASE_TABLE_OPS ="edu.mit.ll.d4m.db.cloud.CloudbaseTableOperations";
	public static String ACCUMULO_TABLE_OPS  ="edu.mit.ll.d4m.db.cloud.AccumuloTableOperations";
	
//	static {
//		String cloud_type = System.getProperty("d4m.cloud.type");
//		log.info("D4M.CLOUD.TYPE = "+cloud_type);
//		if(cloud_type == null) {
//   // Read a config file???
//			
//		}
//	}
	/**
	 * 
	 */
	public D4mFactory() {
		// TODO Auto-generated constructor stub
	}
	
	public static D4mParentQuery createSearcher()
	{
		D4mParentQuery d4m = null;
		CloudbaseQuery cbd4m=null;
		ClassLoader loader = D4mFactory.class.getClassLoader();
		D4mConfig d4mConfig = D4mConfig.getInstance();
		CLOUD_TYPE = d4mConfig.getCloudType();
		try {
			if(CLOUD_TYPE.equals("BigTableLike")) {

				d4m = (D4mParentQuery)loader.loadClass(CLOUDBASE_QUERY2).newInstance();
			}
			else if (CLOUD_TYPE.equals("Accumulo")) {
				d4m = (D4mParentQuery)loader.loadClass(ACCUMULO_QUERY2).newInstance();
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		return d4m;
	}

	/*
	 * Create Query object
	 */
	public static D4mQueryBase createQuery()
	{
		D4mQueryBase d4m = null;
		CloudbaseQuery cbd4m=null;
		ClassLoader loader = D4mFactory.class.getClassLoader();
		D4mConfig d4mConfig = D4mConfig.getInstance();
		CLOUD_TYPE = d4mConfig.getCloudType();
		try {
			if(CLOUD_TYPE.equals("BigTableLike")) {

				d4m = (D4mQueryBase)loader.loadClass(CLOUDBASE_QUERY).newInstance();
			}
			else if (CLOUD_TYPE.equals("Accumulo")) {
				d4m = (D4mQueryBase)loader.loadClass(ACCUMULO_QUERY).newInstance();
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		return d4m;
	}

	/*
	 * Create Query object
	 */
	public static D4mQueryBase createQuery(String instanceName, String host, String table, String username, String password)
	{
		D4mQueryBase d4m = null;
		CloudbaseQuery cbd4m=null;
		ConnectionProperties connProp = new ConnectionProperties();
		connProp.setHost(host);
		connProp.setInstanceName(instanceName);

		connProp.setUser(username);
		connProp.setPass(password);
		ClassLoader loader = D4mFactory.class.getClassLoader();
		try {
			if(CLOUD_TYPE.equals("BigTableLike")) {

				d4m = (D4mQueryBase)loader.loadClass(CLOUDBASE_QUERY).newInstance();
			}
			else if (CLOUD_TYPE.equals("Accumulo")) {
				d4m = (D4mQueryBase)loader.loadClass(ACCUMULO_QUERY).newInstance();
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		d4m.setConnProps(connProp);
		d4m.setTableName(table);

		return d4m;
	}

	public static D4mInsertBase createInserter() {
		D4mInsertBase d4m = null;
		ClassLoader loader = D4mFactory.class.getClassLoader();
		D4mConfig d4mConfig = D4mConfig.getInstance();
		CLOUD_TYPE = d4mConfig.getCloudType();
		try {
			if(CLOUD_TYPE.equals("BigTableLike")) {

				d4m = (D4mInsertBase)loader.loadClass(CLOUDBASE_INSERT).newInstance();
			}
			else if (CLOUD_TYPE.equals("Accumulo")) {
				d4m = (D4mInsertBase)loader.loadClass(ACCUMULO_INSERT).newInstance();
			}
			else {
				log.warn("Cloud Type not recognized" + CLOUD_TYPE);
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		
		return d4m;
	}
	public static D4mInsertBase createInserter(String instanceName, String host, String table, String username, String password) {
		D4mInsertBase d4m = createInserter();
		ConnectionProperties connProp = new ConnectionProperties();
		connProp.setHost(host);
		connProp.setInstanceName(instanceName);

		connProp.setUser(username);
		connProp.setPass(password);
		d4m.setConnProps(connProp);
		d4m.setTableName(table);
		return d4m;

	}
	
	public static D4mTableOpsIF createTableOperations() {
		D4mTableOpsIF d4m = null;
		ClassLoader loader = D4mFactory.class.getClassLoader();
		D4mConfig d4mConfig = D4mConfig.getInstance();
		CLOUD_TYPE = d4mConfig.getCloudType();
		try {
			if(CLOUD_TYPE.equals("BigTableLike")) {

				d4m = (D4mTableOpsIF)loader.loadClass(CLOUDBASE_TABLE_OPS).newInstance();
			}
			else if (CLOUD_TYPE.equals("Accumulo")) {
				d4m = (D4mTableOpsIF)loader.loadClass(ACCUMULO_TABLE_OPS).newInstance();
			}
			else {
				log.warn("Cloud Type not recognized" + CLOUD_TYPE);
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		return d4m;
	}
	public static D4mTableOpsIF createTableOperations(String instanceName, String host, String username, String password) {
		D4mTableOpsIF d4m = null;
		d4m = createTableOperations();
		ConnectionProperties connProp = new ConnectionProperties();
		connProp.setInstanceName(instanceName);
		connProp.setHost(host);
		connProp.setUser(username);
		connProp.setPass(password);
		d4m.setConnProps(connProp);
		d4m.connect();

		return d4m;
	}
}
