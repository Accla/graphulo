/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import org.apache.log4j.Logger;


/**
 * Factory class to create  AccumuloQuery objects
 * @author CHV8091
 *
 */
public class D4mFactory {
	private static Logger log = Logger.getLogger(D4mFactory.class);
	public static String PROPERTY_D4M_CLOUD_TYPE="d4m.cloud.type";

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
		try {
				d4m = (D4mParentQuery)loader.loadClass(ACCUMULO_QUERY2).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}


		return d4m;
	}


	public static D4mInsertBase createInserter() {
		D4mInsertBase d4m = null;
		ClassLoader loader = D4mFactory.class.getClassLoader();
		try {
				d4m = (D4mInsertBase)loader.loadClass(ACCUMULO_INSERT).newInstance();
		} catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
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
		try {
				d4m = (D4mTableOpsIF)loader.loadClass(ACCUMULO_TABLE_OPS).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
		}

		return d4m;
	}
	public static D4mTableOpsIF createTableOperations(String instanceName, String host, String username, String password) throws Exception{
		D4mTableOpsIF d4m = null;
		//System.out.println("about to make table ops");
		d4m = createTableOperations();
		//System.out.println("made table ops");
		ConnectionProperties connProp = new ConnectionProperties();
		connProp.setInstanceName(instanceName);
		connProp.setHost(host);
		connProp.setUser(username);
		connProp.setPass(password);
		d4m.setConnProps(connProp);
		d4m.connect();
		//System.out.println("after connect");

		return d4m;
	}

	public static DbInfoIF createDbInfo(ConnectionProperties connProps) {
		DbInfoIF dbInfo;
		ClassLoader loader = D4mFactory.class.getClassLoader();

		try {
			dbInfo = (DbInfoIF)loader.loadClass(ACCUMULO_DB_INFO).newInstance();
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		dbInfo.setConnectionProp(connProps);

		return dbInfo;
	}
}
