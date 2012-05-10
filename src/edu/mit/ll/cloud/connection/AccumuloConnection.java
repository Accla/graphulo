
/**
 * 
 */
package edu.mit.ll.cloud.connection;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.SortedSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.conf.DefaultConfiguration;

import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.AddressUtil;

import org.apache.accumulo.core.util.ThriftUtil;

import org.apache.accumulo.core.client.impl.Tables;

/**
 * @author cyee
 *
 */
public class AccumuloConnection {
	private static Logger log = Logger.getLogger(AccumuloConnection.class);

	private ConnectionProperties conn=null;
	private ZooKeeperInstance instance=null;
	private Connector connector= null;
	private Authorizations auth= org.apache.accumulo.core.Constants.NO_AUTHS;
	public static long maxMemory= 1024000L;
	public static long maxLatency = 30;
	/**
	 * 
	 */
	public AccumuloConnection(ConnectionProperties conn) {
		this.conn = conn;
		this.instance = new ZooKeeperInstance(conn.getInstanceName(), conn.getHost());
		try {
			this.connector = this.instance.getConnector(this.conn.getUser(), this.conn.getPass().getBytes());
			String [] sAuth = conn.getAuthorizations();
			if (sAuth != null && sAuth.length > 0) {
				auth = new Authorizations(sAuth);
			}

		} catch (AccumuloException e) {
			log.warn("",e);
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			log.warn(e);
			e.printStackTrace();
		}
		
		if(log.isDebugEnabled()) {
			String message="!!!WHOAMI="+this.connector.whoami();
			log.debug(message);
			//System.out.println(message);
		}
	}

	public void createTable(String tableName) {
		try {
			connector.tableOperations().create(tableName);
		} catch (AccumuloException e) {		
			log.warn(e);
		} catch (AccumuloSecurityException e) {

			log.warn(e);
		} catch (TableExistsException e) {
			log.warn("Table "+ tableName+"  exist.",e);
		}
	}

	// batchwriter
	public BatchWriter createBatchWriter (String table, long maxMemory, long maxLatency,int maxWriteThreads) throws TableNotFoundException {
		return connector.createBatchWriter(table, maxMemory, maxLatency, maxWriteThreads);
	}
	public BatchWriter createBatchWriter (String table) throws TableNotFoundException {
		return createBatchWriter(table, maxMemory, maxLatency, conn.getMaxNumThreads());
	}

	//Scanner
	public Scanner createScanner(String tableName) throws TableNotFoundException {
		return this.connector.createScanner(tableName, this.auth);
	}
	//BatchScanner
	public BatchScanner getBatchScanner(String tableName, int numberOfThreads) throws TableNotFoundException  {
		BatchScanner scanner = connector.createBatchScanner(tableName, this.auth, numberOfThreads);
		return scanner;
	}

	public void deleteTable (String tableName)  {
		try {
			connector.tableOperations().delete(tableName);
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			log.warn(e);
		} catch (AccumuloSecurityException e) {
			log.warn(e);
		} catch (TableNotFoundException e) {
			log.warn(e);
		}
	}

	public boolean tableExist(String tableName) {
		return connector.tableOperations().exists(tableName);

	}

	public void addSplit(String tableName, SortedSet<Text> partitions) {
		try {
			connector.tableOperations().addSplits(tableName, partitions);
		} catch (TableNotFoundException e) {

			log.warn(e);
		} catch (AccumuloException e) {

			log.warn(e);
		} catch (AccumuloSecurityException e) {

			log.warn(e);
		}
	}
	public Instance getInstance() {	
		return connector.getInstance();
	}
	
	public MasterClientService.Iface getMasterClient() throws TTransportException {
		return MasterClient.getConnection(getInstance());
	}
	
	public TabletClientService.Iface getTabletClient (String tserverAddress) throws TTransportException {
		InetSocketAddress address = AddressUtil.parseAddress(tserverAddress, -1);
		TabletClientService.Iface client = null;
		client = ThriftUtil.getTServerClient( tserverAddress, connector.getInstance().getConfiguration());
		return client;
	}
	
	public Map<String, String> getNameToIdMap() {
		//Map<String, String> nameToIdMap = Tables.getNameToIdMap(instance);
		Map<String,String> _nameToIdMap = Tables.getNameToIdMap(getInstance());
		
		
		return _nameToIdMap;
	}
	public Collection<Text> getSplits(String tableName) throws TableNotFoundException {
		Collection<Text> splits = this.connector.tableOperations().getSplits(tableName);
		return splits;
	}
	public SortedSet<String> getTableList() {
		
		SortedSet<String> set = this.connector.tableOperations().list();
		return set;
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
