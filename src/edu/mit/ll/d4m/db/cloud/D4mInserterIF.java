/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

/**
 * @author CHV8091
 *
 */
public interface D4mInserterIF {

	public void init(String instanceName, String hostName, String tableName, String username, String password);
	public void doProcessing(String rows, String cols, String vals, String family, String visibility);
}
