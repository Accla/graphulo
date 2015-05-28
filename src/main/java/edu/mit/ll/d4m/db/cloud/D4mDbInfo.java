package edu.mit.ll.d4m.db.cloud;

import edu.mit.ll.cloud.connection.ConnectionProperties;

/**
 * @author wi20909
 */

public class D4mDbInfo extends D4mParent {

	public D4mDbInfo() {
		super();
	}

	private ConnectionProperties connProps = new ConnectionProperties();

	/**
	 * Constructor that may use MasterInstance or ZooKeeperInstance to connect to Accumulo.
	 */
	public D4mDbInfo(ConnectionProperties connProps) {
		this.connProps = connProps;
	}

	/**
	 */
	public D4mDbInfo(String instanceName, String host, String username, String password) {
		this.connProps.setInstanceName(instanceName);
		this.connProps.setHost(host);
		this.connProps.setUser(username);
		this.connProps.setPass(password);
	}

	public String getTableList() throws Exception {
		DbInfoIF dbInfo = D4mFactory.createDbInfo(this.connProps);
		return dbInfo.getTableList();
	}



}
/*
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % D4M: Dynamic Distributed Dimensional Data Model 
 * % MIT Lincoln Laboratory
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % (c) <2010> Massachusetts Institute of Technology
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 */
