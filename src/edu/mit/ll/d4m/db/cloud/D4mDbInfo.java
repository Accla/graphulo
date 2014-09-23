package edu.mit.ll.d4m.db.cloud;

import java.util.Iterator;
import java.util.SortedSet;

import edu.mit.ll.cloud.connection.ConnectionProperties;

/**
 * @author wi20909
 */

public class D4mDbInfo extends D4mParent {

	public D4mDbInfo() {
		super();
	}

	private ConnectionProperties connProps = new ConnectionProperties();

	public String rowReturnString = "";
	public String columnReturnString = "";
	public String valueReturnString = "";

	/**
	 * Constructor that may use MasterInstance or ZooKeeperInstance to connect to Accumulo.
	 * @param connProps
	 */
	public D4mDbInfo(ConnectionProperties connProps) {
		this.connProps = connProps;
	}

	/**
	 * @param instanceName
	 * @param host
	 * @param username
	 * @param password
	 */
	public D4mDbInfo(String instanceName, String host, String username, String password) {
		this.connProps.setInstanceName(instanceName);
		this.connProps.setHost(host);
		this.connProps.setUser(username);
		this.connProps.setPass(password);
	}

	public String getTableList() throws Exception {

		String s=null;
		DbInfoIF dbInfo = null;
		dbInfo = D4mFactory.createDbInfo(this.connProps);
		s = dbInfo.getTableList();

		return s;
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
