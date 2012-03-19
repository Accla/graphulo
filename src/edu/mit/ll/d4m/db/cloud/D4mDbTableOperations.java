package edu.mit.ll.d4m.db.cloud;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Map;
import java.util.SortedMap;

import org.apache.hadoop.io.Text;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.TException;

import cloudbase.core.conf.CBConfiguration;
import cloudbase.core.client.Instance;
import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.TableExistsException;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.client.impl.MasterClient;
import cloudbase.core.client.impl.Tables;
import cloudbase.core.client.impl.ThriftTransportPool;
import cloudbase.core.data.Key;
import cloudbase.core.data.Value;
import cloudbase.core.master.thrift.MasterClientService;
import cloudbase.core.master.thrift.MasterMonitorInfo;
import cloudbase.core.master.thrift.TabletInfo;
import cloudbase.core.master.thrift.TabletServerStatus;
import cloudbase.core.security.thrift.AuthInfo;
import cloudbase.core.security.thrift.ThriftSecurityException;
import cloudbase.core.tabletserver.thrift.TabletClientService;
import cloudbase.core.tabletserver.thrift.TabletStats;
import cloudbase.core.util.AddressUtil;
import cloudbase.core.util.ThriftUtil;
import cloudbase.core.master.thrift.TableInfo;


import edu.mit.ll.cloud.connection.CloudbaseConnection;
import edu.mit.ll.cloud.connection.ConnectionProperties;



/**
 * @author wi20909
 */
public class D4mDbTableOperations {
	private static  Logger log = Logger.getLogger(D4mDbTableOperations.class.getName());
	public String rowReturnString = "";
	public String columnReturnString = "";
	public String valueReturnString = "";
	private long timeout=120000l; //120s
	private ConnectionProperties connProps = new ConnectionProperties();

	D4mTableOpsIF d4mTableOp = null;
	public D4mDbTableOperations() {

	}

	public D4mDbTableOperations(ConnectionProperties connProps) {
		this.connProps = connProps;
	}
	public D4mDbTableOperations(String instanceName, String host, String username, String password) {

	}

	public D4mDbTableOperations(String instanceName, String host, String username, String password, String cloudType) {

	    init(instanceName, host,username,password,cloudType);

	}

    public void init(String instanceName, String host, String username, String password,String cloudType) {
		this.connProps.setHost(host);
		this.connProps.setInstanceName(instanceName);
		this.connProps.setUser(username);
		this.connProps.setPass(password);
		setCloudType(cloudType);
		doInit();
	}
	private void doInit() {
		String instanceName = this.connProps.getInstanceName();
		String host = this.connProps.getHost();
		String username = this.connProps.getUser();
		String password = this.connProps.getPass();
		d4mTableOp = D4mFactory.createTableOperations(instanceName, host, username, password);
		
	}

	public void createTable(String tableName) {
		this.d4mTableOp.createTable(tableName);
		//		CloudbaseConnection cbConnection = null;
		//		try {
		//			cbConnection = new CloudbaseConnection(this.connProps);
		//		}
		//		catch (CBException ex) {
		//			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		//		}
		//		catch (CBSecurityException ex) {
		//			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		//		}
		//		try {
		//			cbConnection.createTable(tableName);
		//			System.out.println("The " + tableName + " table was created.");
		//		}
		//		catch (CBException ex) {
		//			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		//		}
		//		catch (CBSecurityException ex) {
		//			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		//		}
		//		catch (TableExistsException ex) {
		//			System.out.println("The " + tableName + " table already Exists.");
		//		}
	}

	public void deleteTable(String tableName) {
		this.d4mTableOp.deleteTable(tableName);
		//		CloudbaseConnection cbConnection = null;
		//		try {
		//			cbConnection = new CloudbaseConnection(this.connProps);
		//		}
		//		catch (CBException ex) {
		//			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		//		}
		//		catch (CBSecurityException ex) {
		//			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		//		}
		//		try {
		//			cbConnection.deleteTable(tableName);
		//			System.out.println("The " + tableName + " table was deleted.");
		//		}
		//		catch (CBException ex) {
		//			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		//		}
		//		catch (CBSecurityException ex) {
		//			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		//		}
		//		catch (TableNotFoundException ex) {
		//			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		//		}
	}

	/*
	 *  tableName  name of table to split
	 *  partitionKey     a string or comma-separated list
	 */
	public void splitTable(String tableName, String partitionKey)  throws IOException, CBException, CBSecurityException, TableNotFoundException {
		//	String [] pKeys = partitionKey.split(",");
		//	//System.out.println(" *** Number of partition keys = "+ pKeys.length);
		//	splitTable(tableName,pKeys);
		this.d4mTableOp.splitTable(tableName, partitionKey);
	}

	/*
	 *  tableName  name of table to split
	 *  partitionKeys  array of strings
	 */
	public void splitTable(String tableName, String [] partitionKeys)  throws IOException, CBException, CBSecurityException, TableNotFoundException {
//		ArrayList<String> list = new ArrayList<String>();
//		for(int i =0; i < partitionKeys.length; i++) {
//			list.add(partitionKeys[i]);
//		}
//		splitTable(tableName, list);
		this.d4mTableOp.splitTable(tableName, partitionKeys);
	}

	/*
	 *  tableName  name of table to split
	 *   partitionKeys   - list of keys (eg.  java.util.ArrayList)
	 */
	public void splitTable(String tableName, List<String> partitionKeys) throws IOException, CBException, CBSecurityException, TableNotFoundException {
		TreeSet<Text> tset = new TreeSet<Text>();
		
		for(String pt : partitionKeys) {
			tset.add(new Text(pt));
		}
		
		this.d4mTableOp.splitTable(tableName, tset);
//		CloudbaseConnection  cbConnection = new CloudbaseConnection(this.connProps);
//		cbConnection.splitTable(tableName, partitionKeys);
	}

	public void setCloudType(String cloudType) {
		D4mConfig d4mconf = D4mConfig.getInstance();
		d4mconf.setCloudType(cloudType);
	}
	/*
	 *
	 *  tserverAddress    host:port
	 *  tableNamesList   list of the table names
	 *
	 */
	
	/*
	public List<TabletStats> getTabletStatsList(String tserverAddress, List<String> tableNamesList) throws CBException, CBSecurityException, TableNotFoundException  {
		InetSocketAddress address = AddressUtil.parseAddress(tserverAddress, -1);
		List<TabletStats> tsStats = new ArrayList<TabletStats>();
		Instance instance = connection().getInstance();

		Map<String, String> nameToIdMap = Tables.getNameToIdMap(instance);    
		CBConfiguration cbConf = CBConfiguration.getSystemConfiguration(instance);

		try {
			TabletClientService.Iface client = ThriftUtil.getClient(new TabletClientService.Client.Factory(),
					address, cbConf);
			try {
				for(String tableName: tableNamesList) {
					String tableId = nameToIdMap.get(tableName);
					//		for (String tableId : mmi.tableMap.keySet()) {
					tsStats.addAll(client.getTabletStats(null, authInfo(), tableId));
				}

			} finally {
				ThriftUtil.returnClient(client);
			}
		} catch (Exception e) {
			log.fine( e.toString());

		}

		return tsStats;
	}
	*/

	/*
	 *  Return the number of entries in this cloud instance.
	 *  This method will give a total number of entries from all tables in this cloud instance
	 *
	 *   tserverAddress   address (host:port) of tserver 
	 */
/*
	public long getNumberOfEntries(String tserverAddress) throws CBException, CBSecurityException, TableNotFoundException  {

		long retValue=0;

		AuthInfo authInfo = authInfo();
		CloudbaseConnection connector = connection();
		Instance instance = connector.getInstance();
		CBConfiguration cbConf = CBConfiguration.getSystemConfiguration(instance);

		InetSocketAddress address = AddressUtil.parseAddress(tserverAddress, -1);
		MasterMonitorInfo mmi = getMmi();
		TabletClientService.Iface client=null;
		List<TabletStats> tsStats = new ArrayList<TabletStats>();

		try {
			client = ThriftUtil.getClient(new TabletClientService.Client.Factory(),
					address, cbConf);

			for (String tableId : mmi.tableMap.keySet()) {
				System.out.println("Get TableId="+tableId);

				tsStats.addAll(client.getTabletStats(null, authInfo, tableId));


			}
			for (TabletStats info : tsStats) {	      
				retValue = info.numEntries;
			}
		}
		catch(TException e) {
			log.fine(e.toString());
		}

		catch(ThriftSecurityException e) {
			log.fine(e.toString());
		}
		finally {
			ThriftUtil.returnClient(client);
		}
		return retValue;
	}

*/
/*
	private MasterMonitorInfo getMmi() {
		MasterMonitorInfo mmi = null;
		MasterClientService.Iface masterclient = null;
		try {
			CloudbaseConnection connector = connection();
			Instance instance = connector.getInstance();
			masterclient = MasterClient.getConnection(instance);
			mmi=  masterclient.getMasterStats(null, authInfo());
		}
		catch(Exception e) {
			log.fine(e.toString());
		}
		finally {
			ThriftUtil.returnClient(masterclient);
		}
		return mmi;
	}
*/
//	private String getTableName(String tabletId) throws CBException, CBSecurityException, TableNotFoundException  {
//		CloudbaseConnection connector = connection();
//		return Tables.getTableName(connector.getInstance(), tabletId);
//	}

	/*
	 *
	 * tserverAddress   tablet server  address (by name) IP:port
	 * tableName    table name
	 *
	 *  RETURN a negative (-1) if there is an error, otherwise 
	 */
//	public long getNumberOfEntries(String tserverAddress, String tableName) throws CBException, CBSecurityException, TableNotFoundException  {
//		long retValue=0;
//		ArrayList<String> tmpList = new ArrayList<String>();
//		tmpList.add(tableName);
//		retValue = getNumberOfEntries(tserverAddress, tmpList);
//
//		return retValue;
//	}

	/**
	 *  Return the number of entries in this cloud instance.
	 *  This method will give a total number of entries from all tablets in this cloud instance.
	 */
	/*
	public long getNumberOfEntries() throws CBException, CBSecurityException, TableNotFoundException {
		//The MasterMonitorInfo holds the tserver's info
		//mmi will have a list of tservers
		MasterMonitorInfo mmi=null;  
		AuthInfo authInfo = authInfo();
		CloudbaseConnection connector = connection();
		Instance instance = connector.getInstance();
		long retValue= 0l;
		SortedMap<String, TableInfo> tableStats = new TreeMap<String, TableInfo>();
		Map<String, String> tidToNameMap = Tables.getIdToNameMap(instance);

		try {
			mmi = getMmi();
			for (Entry<String, TableInfo> te : mmi.tableMap.entrySet())
				tableStats.put(Tables.getPrintableTableNameFromId(tidToNameMap, te.getKey()), te.getValue());

			for (Entry<String, String> tableName_tableId : Tables.getNameToIdMap(instance).entrySet()) {
				String tableName = tableName_tableId.getKey();
				String tableId = tableName_tableId.getValue();
				TableInfo tableInfo = tableStats.get(tableName);
				retValue += tableInfo.recs;
				log.fine("TABLE_NAME="+tableName+", TABLE_ID="+tableId+",NumRecs="+tableInfo.recs);
			}
			log.fine("tss numRecords = "+ retValue);
		}	
		catch (Exception e) {
			mmi = null;

		}

		return retValue;
	}
*/
	/*
	 *  Get a list of tablet servers
	 *
	 */
	/*
	private ArrayList<TabletServerStatus> getTabletServers()  throws CBException, CBSecurityException, TableNotFoundException,TTransportException {
		MasterMonitorInfo mmi=null;  
		AuthInfo authInfo = authInfo();
		CloudbaseConnection connector = connection();

		MasterClientService.Iface client = null;
		ArrayList<TabletServerStatus> tservers = new ArrayList<TabletServerStatus>();
		try {
			client = MasterClient.getConnection(connector.getInstance());
			mmi = client.getMasterStats(null,authInfo);
			if (mmi != null)
				tservers.addAll(mmi.tServerInfo);

		} catch (Exception e) {
			mmi = null;

		} finally {
			if (client != null)
				ThriftUtil.returnClient(client);
		}
		return tservers;
	}
*/
	/*
	 *
	 *
	 *
	 */
//	public long getNumberOfEntries(String tserverAddress, ArrayList<String>  tableNames) throws CBException, CBSecurityException, TableNotFoundException {
//		long retval =0l;
//		List<TabletStats> tsStats = getTabletStatsList(tserverAddress, tableNames);
//		for(TabletStats info : tsStats) {
//			retval += info.numEntries;
//		}
//
//		return retval;
//	}
	/*  
	 *  Get the total number of entries for the specified table names
	 *  tableNames   list of table names of interest	
	 */
	public long getNumberOfEntries(ArrayList<String>  tableNames) throws CBException, CBSecurityException, TableNotFoundException, TTransportException {
		long retVal= this.d4mTableOp.getNumberOfEntries(tableNames);

//		ArrayList<TabletServerStatus> tservers = getTabletServers();
//		for (TabletServerStatus status : tservers) {
//			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.FINE,"TabletServer status ::  name = "+status.name);
//			retVal += getNumberOfEntries(status.name, tableNames);
//		}

		return retVal;
	}
	/*
	 *  Count the number of rows in this table of this cloud instance
	 *
	 */
	/*
	public long getNumberOfRows(String tableName) throws CBException, TableNotFoundException, CBSecurityException {
		long retVal=0;
		CloudbaseConnection cbConnection = connection();
		Scanner scanner = cbConnection.getScanner(tableName);
		//HashSet<Range> ranges = new HashSet<Range>(1);
		//Text row1 = new Text("*");
		//Range range = new Range(new Text(row1));
		//ranges.add(range);
		//String regexParams = ".";
		//scanner.setRowRegex(regexParams);
		//scanner.setRanges(ranges);
		Iterator<Entry<Key,Value>> scannerIter = scanner.iterator();
		while (scannerIter.hasNext()) {
			Entry<Key, Value> entry = (Entry<Key, Value>) scannerIter.next();
			if (entry != null) {
				retVal++;
			}
		}

		return retVal;
	}
*/
	/*
	 *  Count the total number of rows of all tables in this cloud instance
	 *
	 */
	/*
	public long getNumberOfRows() throws CBException, TableNotFoundException, CBSecurityException {
		long retVal=0;
		CloudbaseConnection connector = connection();

		//	Map<String, String> nameToIdMap = Tables.getNameToIdMap(connector.getInstance());
		for (Entry<String, String> tableName_tableId : Tables.getNameToIdMap(connector.getInstance()).entrySet()) {
			String tableName = tableName_tableId.getKey();
			String tableId = tableName_tableId.getValue();
			log.info ("TableName = "+tableName+ ",  TableId="+ tableId);
			if(!tableName.equals("!METADATA"))
				retVal += getNumberOfRows(tableName);
		}
		return retVal;
	}
*/
	
	public List<String> getSplits(String tableName){
		List<String> list = this.d4mTableOp.getSplits(tableName);
			
//			new ArrayList<String>();
//		CloudbaseConnection connector = connection();
//		Collection<Text> splitsList = connector.getSplits(tableName);
//
//		for(Text txt : splitsList) {
//			String s =new String( txt.getBytes());
//			list.add(s);
//		}
		return list;
	}
//	private CloudbaseConnection connection() throws CBException, TableNotFoundException, CBSecurityException {
//		CloudbaseConnection connector = new CloudbaseConnection(this.connProps);
//		return connector;
//	}
//	private AuthInfo authInfo() throws CBException, TableNotFoundException, CBSecurityException {
//		String user = this.connProps.getUser();
//		byte[] pass = this.connProps.getPass().getBytes();
//		CloudbaseConnection connector = connection();
//		return 	 new AuthInfo(user,pass, connector.getInstanceID());
//
//	}

}
/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% D4M: Dynamic Distributed Dimensional Data Model
% MIT Lincoln Laboratory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% (c) <2010> Massachusetts Institute of Technology
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 */

