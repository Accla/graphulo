/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.log4j.Logger;

import edu.mit.ll.cloud.connection.ConnectionProperties;


/**
 * Factory class to create  AccumuloQuery objects
 * @author CHV8091
 *
 */
public class D4mFactory {
	private static Logger log = Logger.getLogger(D4mFactory.class);

	public static String ACCUMULO= D4mConfig.ACCUMULO;
	public static String PROPERTY_D4M_CLOUD_TYPE="d4m.cloud.type";
	public static String CLOUD_TYPE="BigTableLike"; // BigTableLike or Accumulo

	public static String ACCUMULO_QUERY2="edu.mit.ll.d4m.db.cloud.accumulo.D4mDbQueryAccumulo";

	public static String ACCUMULO_INSERT="edu.mit.ll.d4m.db.cloud.accumulo.AccumuloInsert";

	public static String ACCUMULO_TABLE_OPS  ="edu.mit.ll.d4m.db.cloud.accumulo.AccumuloTableOperations";


	public static String ACCUMULO_DB_INFO="edu.mit.ll.d4m.db.cloud.accumulo.AccumuloInfo";

	/**
	 * 
	 */
	public D4mFactory() {

	}

	public static D4mParentQuery createSearcher()
	{
		D4mParentQuery d4m = null;
		ClassLoader loader = D4mFactory.class.getClassLoader();
		D4mConfig d4mConfig = D4mConfig.getInstance();
		CLOUD_TYPE = d4mConfig.getCloudType();
		try {

		        if (CLOUD_TYPE.equals(ACCUMULO)) {
				d4m = (D4mParentQuery)loader.loadClass(ACCUMULO_QUERY2).newInstance();
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}


		return d4m;
	}


	public static D4mInsertBase createInserter() {
		D4mInsertBase d4m = null;
		ClassLoader loader = D4mFactory.class.getClassLoader();
		D4mConfig d4mConfig = D4mConfig.getInstance();
		CLOUD_TYPE = d4mConfig.getCloudType();
		try {

			if (CLOUD_TYPE.equals(ACCUMULO)) {
				d4m = (D4mInsertBase)loader.loadClass(ACCUMULO_INSERT).newInstance();
			}
			else {
				log.warn("Cloud Type not recognized" + CLOUD_TYPE);
			}
		} catch (InstantiationException e) {

			e.printStackTrace();
		} catch (IllegalAccessException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {

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

		        if (CLOUD_TYPE.equals(ACCUMULO)) {
				d4m = (D4mTableOpsIF)loader.loadClass(ACCUMULO_TABLE_OPS).newInstance();
			}
			else {
				log.warn("Cloud Type not recognized" + CLOUD_TYPE);
			}
		} catch (InstantiationException e) {

			e.printStackTrace();
		} catch (IllegalAccessException e) {

			e.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		}

		return d4m;
	}
	public static D4mTableOpsIF createTableOperations(String instanceName, String host, String username, String password) {
		D4mTableOpsIF d4m = null;
		System.out.println("about to make table ops");
		d4m = createTableOperations();
		System.out.println("made table ops");
		ConnectionProperties connProp = new ConnectionProperties();
		connProp.setInstanceName(instanceName);
		connProp.setHost(host);
		connProp.setUser(username);
		connProp.setPass(password);
		d4m.setConnProps(connProp);
		d4m.connect();
		System.out.println("after connect");

		return d4m;
	}

	public static DbInfoIF createDbInfo(ConnectionProperties connProps) {
		DbInfoIF dbInfo= null;
		D4mConfig d4mConfig = D4mConfig.getInstance();
		CLOUD_TYPE = d4mConfig.getCloudType();
		ClassLoader loader = D4mFactory.class.getClassLoader();

		try {

			if (CLOUD_TYPE.equals(ACCUMULO)) {

				dbInfo = (DbInfoIF)loader.loadClass(ACCUMULO_DB_INFO).newInstance();
			}
			else {
				log.warn("Cloud Type not recognized" + CLOUD_TYPE);
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}  catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
		dbInfo.setConnectionProp(connProps);

		return dbInfo;
	}
}
