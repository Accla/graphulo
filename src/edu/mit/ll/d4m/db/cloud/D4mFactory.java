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
	public static String ACCUMULO_QUERY="edu.mit.ll.d4m.cloud.AccumuloQuery";

	static {
		String cloud_type = System.getProperty("d4m.cloud.type");
		log.info("D4M.CLOUD.TYPE = "+cloud_type);
		if(cloud_type == null) {
// Read a config file???
			
		}
	}
	/**
	 * 
	 */
	public D4mFactory() {
		// TODO Auto-generated constructor stub
	}
	public static D4mQueryBase create()
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

	public static D4mQueryBase create(String instanceName, String host, String table, String username, String password)
	{
		D4mQueryBase d4m = null;
		CloudbaseQuery cbd4m=null;
		ConnectionProperties connProp = new ConnectionProperties();
		connProp.setHost(host);
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

}
