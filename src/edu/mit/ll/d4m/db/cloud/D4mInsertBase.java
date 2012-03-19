/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.util.MutationSorter;

/**
 * @author CHV8091
 *
 */
public abstract class D4mInsertBase implements D4mInserterIF {
	protected String tableName = "";
	protected String rows = "";
	protected String cols = "";
	protected String vals = "";
	protected String family = "";
	protected String visibility = "";

	protected ConnectionProperties connProps = new ConnectionProperties();
	protected MutationSorter mutSorter = new MutationSorter();
	
	public D4mInsertBase() {
		
	}
	public D4mInsertBase(String instanceName,String hostName, String tableName, String username, String password) {
		init(instanceName,hostName,tableName,username,password);
	}

	@Override
	public void init(String instanceName, String hostName, String tableName,
			String username, String password) {
		connProps.setInstanceName(instanceName);
		connProps.setHost(hostName);
		connProps.setUser(username);
		connProps.setPass(password);
		this.tableName = tableName;

		
	}

	@Override
	public void doProcessing(String rows, String cols, String vals,
			String family, String visibility) {
		// TODO Auto-generated method stub
		this.rows = rows;
		this.cols = cols;
		this.vals = vals;
		this.family = family;
		this.visibility = visibility;
		doProcessing();
	}
	
	abstract public void doProcessing();
	
	public void setConnProps(ConnectionProperties connProps) {
		this.connProps = connProps;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
}

/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% D4M: Dynamic Distributed Dimensional Data Model
% MIT Lincoln Laboratory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% (c) <2010> Massachusetts Institute of Technology
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 */

