/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import edu.mit.ll.cloud.connection.ConnectionProperties;

/**
 * Interface for D4mDbInfo
 * @author cyee
 *
 */
public interface DbInfoIF {

	/**
	 *  Get a list of tables
	 * @return  String
	 */
	public String getTableList() throws Exception;
	public void setConnectionProp(ConnectionProperties connProps);
}
