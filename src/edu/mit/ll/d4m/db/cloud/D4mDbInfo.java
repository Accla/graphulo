package edu.mit.ll.d4m.db.cloud;

import java.util.Iterator;
import java.util.SortedSet;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.TableNotFoundException;
import edu.mit.ll.cloud.connection.CloudbaseConnection;
import edu.mit.ll.cloud.connection.ConnectionProperties;

/**
 * @author wi20909
 */

public class D4mDbInfo {

	public D4mDbInfo() {
	}

	private ConnectionProperties connProps = new ConnectionProperties();

	public String rowReturnString = "";
	public String columnReturnString = "";
	public String valueReturnString = "";

	/**
	 * Constructor that may use MasterInstance or ZooKeeperInstance to connect to CB.
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

	public static void main(String[] args) throws CBException, CBSecurityException, TableNotFoundException {
		if (args.length < 1) {
			return;
		}

		//String hostName = args[0];
		//D4mDbInfo ci = new D4mDbInfo("", hostName, "root", "secret");
		//String tableList = ci.getTableList();
		//System.out.println(ci.connProps.getUser() + " " + ci.connProps.getPass());
		//System.out.println(tableList);
	}

	public String getTableList() throws CBException, CBSecurityException {
		CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);

		SortedSet<?> set = cbConnection.getTableList();
		Iterator<?> it = set.iterator();
		StringBuilder sb = new StringBuilder();
		while (it.hasNext()) {
			String tableName = (String) it.next();
			sb.append(tableName + " ");
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
