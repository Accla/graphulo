package edu.mit.ll.d4m.db.cloud;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.accumulo.AccumuloConnection;

import java.util.Iterator;
import java.util.SortedSet;

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
		AccumuloConnection connection = new AccumuloConnection(this.connProps);

		SortedSet<String> set = connection.getTableList();
		Iterator<String> it = set.iterator();
		StringBuilder sb = new StringBuilder();
		while (it.hasNext()) {
			String tableName = it.next();
			sb.append(tableName).append(" ");
		}
		return sb.toString();
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
