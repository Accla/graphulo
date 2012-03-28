/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.util.D4mDataObj;

/**
 * @author cyee
 *
 */
public abstract class D4mParentQuery extends D4mParent {

	protected String tableName=null;
	protected ConnectionProperties connProps=null;
	protected int limit=0;
	
	/**
	 * 
	 */
	public D4mParentQuery() {
		connProps = new ConnectionProperties();
	}

	/**
	 * @param rows
	 * @param cols
	 * @param family
	 * @param authorizations
	 * @return
	 * @throws D4mException
	 */
	abstract public D4mDbResultSet doMatlabQuery(String rows, String cols, String family, String authorizations) throws D4mException;
	abstract public void next();
	abstract public boolean hasNext();
	abstract public D4mDataObj getResults();
	abstract public void reset();
//	public void setCloudType(String cloudType) {
//		D4mConfig d4mConfig = D4mConfig.getInstance();
//		d4mConfig.setCloudType(cloudType);
//	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public ConnectionProperties getConnProps() {
		return connProps;
	}

	public void setConnProps(ConnectionProperties connProps) {
		this.connProps = connProps;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}
}
