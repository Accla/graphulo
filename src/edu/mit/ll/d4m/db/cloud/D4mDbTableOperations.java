package edu.mit.ll.d4m.db.cloud;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.Set;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import cloudbase.core.client.Scanner;
import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.TableExistsException;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.CBConstants;
import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.Connector;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;
import cloudbase.core.security.thrift.AuthInfo;
import cloudbase.core.client.ZooKeeperInstance;

import cloudbase.core.client.impl.HdfsZooInstance;
import cloudbase.core.client.impl.Tables;
import cloudbase.core.client.impl.ThriftTransportPool;
import cloudbase.core.master.thrift.TableInfo;
import cloudbase.core.master.thrift.TabletInfo;
import cloudbase.core.master.thrift.TabletRates;
import cloudbase.core.master.thrift.TabletServerStatus;
import cloudbase.core.tabletserver.thrift.TabletClientService;
import cloudbase.core.util.AddressUtil;
import cloudbase.core.util.Pair;
import cloudbase.server.master.mgmt.TabletServerState;
import cloudbase.server.monitor.Monitor;
import cloudbase.server.monitor.Monitor.MajorMinorStats;
import cloudbase.server.monitor.util.Table;
import cloudbase.server.monitor.util.TableRow;
import cloudbase.server.monitor.util.celltypes.CompactionsType;
import cloudbase.server.monitor.util.celltypes.DurationType;
import cloudbase.server.monitor.util.celltypes.NumberType;
import cloudbase.server.monitor.util.celltypes.ProgressChartType;
import cloudbase.server.monitor.util.celltypes.TServerLinkType;
import cloudbase.server.monitor.util.celltypes.TableLinkType;
import cloudbase.server.security.SecurityConstants;
import cloudbase.core.master.thrift.MasterClientService;
import cloudbase.core.master.thrift.MasterMonitorInfo;
import cloudbase.core.client.impl.MasterClient;

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

    private static int NUM_THREADS=3;
	private ConnectionProperties connProps = new ConnectionProperties();

	public D4mDbTableOperations() {
	}
	
	public D4mDbTableOperations(ConnectionProperties connProps) {
		this.connProps = connProps;
	}
	
	public D4mDbTableOperations(String instanceName, String host, String username, String password) {
		this.connProps.setHost(host);
		this.connProps.setInstanceName(instanceName);
		this.connProps.setUser(username);
		this.connProps.setPass(password);
	}

	public void createTable(String tableName) {
		CloudbaseConnection cbConnection = null;
		try {
			cbConnection = new CloudbaseConnection(this.connProps);
		}
		catch (CBException ex) {
			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		}
		catch (CBSecurityException ex) {
			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		}
		try {
			cbConnection.createTable(tableName);
			System.out.println("The " + tableName + " table was created.");
		}
		catch (CBException ex) {
			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		}
		catch (CBSecurityException ex) {
			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		}
		catch (TableExistsException ex) {
			System.out.println("The " + tableName + " table already Exists.");
		}
	}

	public void deleteTable(String tableName) {
		CloudbaseConnection cbConnection = null;
		try {
			cbConnection = new CloudbaseConnection(this.connProps);
		}
		catch (CBException ex) {
			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		}
		catch (CBSecurityException ex) {
			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		}
		try {
			cbConnection.deleteTable(tableName);
			System.out.println("The " + tableName + " table was deleted.");
		}
		catch (CBException ex) {
			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		}
		catch (CBSecurityException ex) {
			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		}
		catch (TableNotFoundException ex) {
			Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

    /*
     *  tableName  name of table to split
     *  partitionKey     a string or comma-separated list
     */
    public void splitTable(String tableName, String partitionKey)  throws IOException, CBException, CBSecurityException, TableNotFoundException {
	String [] pKeys = partitionKey.split(",");
	//System.out.println(" *** Number of partition keys = "+ pKeys.length);
	splitTable(tableName,pKeys);
    }

    /*
     *  tableName  name of table to split
     *  partitionKeys  array of strings
     */
    public void splitTable(String tableName, String [] partitionKeys)  throws IOException, CBException, CBSecurityException, TableNotFoundException {
	ArrayList<String> list = new ArrayList<String>();
	for(int i =0; i < partitionKeys.length; i++) {
	    list.add(partitionKeys[i]);
	}
	splitTable(tableName, list);
    }

    /*
     *  tableName  name of table to split
     *   partitionKeys   - list of keys (eg.  java.util.ArrayList)
     */
    public void splitTable(String tableName, List<String> partitionKeys) throws IOException, CBException, CBSecurityException, TableNotFoundException {

	CloudbaseConnection  cbConnection = new CloudbaseConnection(this.connProps);
	cbConnection.splitTable(tableName, partitionKeys);
    }

    /*
     *  Return the number of entries in this cloud instance.
     *  This method will give a total number of entries from all tables in this cloud instance
     *
     *   tserverAddress   address (host:port) of tserver 
     */
    public long getNumberOfEntries(String tserverAddress) throws CBException, CBSecurityException, TableNotFoundException  {

	long retValue=0;

	AuthInfo authInfo = authInfo();
	CloudbaseConnection connector = connection();
	
	Map<String, String> nameToIdMap = Tables.getNameToIdMap(connector.getInstance());
	InetSocketAddress address = AddressUtil.parseAddress(tserverAddress, -1);
	TTransport transport = null;
	ThriftTransportPool transportPool = ThriftTransportPool.getInstance();
	TabletInfo total = new TabletInfo();
	TreeMap<String, TabletInfo> tabletMap =null;

	try {
	    Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.FINE," address = "+ address.getAddress().getHostAddress()+ ", port = "+ address.getPort());
	    transport = transportPool.getTransportWithDefaultTimeout(address.getAddress().getHostAddress(), address.getPort());

	    // Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.FINE,"Got a transport obj.");
	    TProtocol protocol = new TBinaryProtocol(transport);
	    // Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.FINE,"Got a TProtocol obj.");
	    TabletClientService.Client client = new TabletClientService.Client(protocol);
	    // Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.FINE,"Got a TabletClientService client.");
	
	    tabletMap = new TreeMap<String, TabletInfo>(client.getTabletMap(authInfo));

	    /*
	    int size= tabletMap.size();
	    Set<String> keySet = tabletMap.keySet();
	    Iterator<String> itKey = keySet.iterator();
	    while(itKey.hasNext()) {
		String k = itKey.next();
		Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.FINE,"TabletMap key = "+ k);
	    }
	    */
	    int skipped=0;
	    int numTabletInSet=0;
	    for (Entry<String, TabletInfo> te : tabletMap.entrySet()) {
		numTabletInSet++;
		Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.FINE," Entry key = "+ te.getKey());
		TabletInfo info = te.getValue();


		String[] tabletIdent = te.getKey().split(";");
		String thisTableName=null;

		try {
		    thisTableName = Tables.getTableName(connector.getInstance(), tabletIdent[0]);
		} catch (TableNotFoundException e) {
		    //log.warning(tabletIdent[0]+ e);
		    //continue;
		}

		if (te.getKey().isEmpty()) {
		    log.fine("TABLE="+thisTableName+", tablet skipping ="+ te.getKey());
		    skipped++;
		    continue;
		}
		log.fine("Tablet name ["+te.getKey() + "] has  "+ info.numEntries+ "  entries.");
		total.numEntries += info.numEntries;
	    }
	    log.fine("*** Number of tablets = "+numTabletInSet + ", number of tablets skipped = " + skipped);
	    Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.FINE,"***** Total number of entries = " + total.numEntries);
	    //	    tabletMap = new TreeMap<String, TabletInfo>(client.getTabletMap(SecurityConstants.systemCredentials));

	} catch (Exception e) {
	    //			banner(sb, "error", "No&nbsp;Such&nbsp;Tablet&nbsp;Server&nbsp;Available");
	    Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE,"",e);
	    return 0;
	} finally {
	    transportPool.returnTransport(transport);
	}
	retValue = total.numEntries;

	return retValue;
	
    }

    private TreeMap<String, TabletInfo>  getTabletInfo(String tserverAddress)  throws CBException, CBSecurityException, TableNotFoundException  {
	AuthInfo authInfo = authInfo();
	CloudbaseConnection connector = connection();
	
	//	Map<String, String> nameToIdMap = Tables.getNameToIdMap(connector.getInstance());
	InetSocketAddress address = AddressUtil.parseAddress(tserverAddress, -1);
	TTransport transport = null;
	ThriftTransportPool transportPool = ThriftTransportPool.getInstance();
	TabletInfo total = new TabletInfo();
	TreeMap<String, TabletInfo> tabletMap =null;

	try {
	   log.fine(" address = "+ 
		    address.getAddress().getHostAddress()+
		    ", port = "+ address.getPort());
	    transport = transportPool.getTransportWithDefaultTimeout(address.getAddress().getHostAddress(), address.getPort());

	    // Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.FINE,"Got a transport obj.");
	    TProtocol protocol = new TBinaryProtocol(transport);
	    // Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.FINE,"Got a TProtocol obj.");
	    TabletClientService.Client client = new TabletClientService.Client(protocol);
	    // Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.FINE,"Got a TabletClientService client.");
	
	    tabletMap = new TreeMap<String, TabletInfo>(client.getTabletMap(authInfo));
	
	} catch (Exception e) {
	    Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE,"",e);
	    return null;
	} finally {
	    transportPool.returnTransport(transport);
	}

	return tabletMap;
    }

    private String getTableName(String tabletId) throws CBException, CBSecurityException, TableNotFoundException  {
	AuthInfo authInfo = authInfo();
	CloudbaseConnection connector = connection();
	return Tables.getTableName(connector.getInstance(), tabletId);
    }

    /*
     *
     * tserverAddress   tablet server  address (by name)
     * tableName    table name
     *
     *  RETURN a negative (-1) if there is an error, otherwise 
     */
    public long getNumberOfEntries(String tserverAddress, String tableName) throws CBException, CBSecurityException, TableNotFoundException  {
	long retValue=0;
	ArrayList<String> tmpList = new ArrayList<String>();
	tmpList.add(tableName);
	retValue = getNumberOfEntries(tserverAddress, tmpList);

	return retValue;
    }

    /**
     *  Return the number of entries in this cloud instance.
     *  This method will give a total number of entries from all tablets in this cloud instance.
     */
    public long getNumberOfEntries() throws CBException, CBSecurityException, TableNotFoundException {
	//The MasterMonitorInfo holds the tserver's info
	//mmi will have a list of tservers
	MasterMonitorInfo mmi=null;  
	AuthInfo authInfo = authInfo();
	CloudbaseConnection connector = connection();

	long retValue= 0;
	long numRecords=0;
	MasterClientService.Client client = null;
	try {
	    client = MasterClient.getConnection(connector.getInstance());
	    mmi = client.getMasterStats(authInfo);
	    ArrayList<TabletServerStatus> tservers = new ArrayList<TabletServerStatus>();
	    if (mmi != null)
		tservers.addAll(mmi.tServerInfo);
	    for (TabletServerStatus status : tservers) {
		Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.FINE,"TabletServer status ::  name = "+status.name);
		retValue += getNumberOfEntries(status.name);
		numRecords += status.totalRecords;
	    }
	    log.fine("tss numRecords = "+numRecords);

	} catch (Exception e) {
	    mmi = null;

	} finally {
	    if (client != null)
		MasterClient.close(client);
	}

	return retValue;
    }

    /*
     *  Get a list of tablet servers
     *
     */
    private ArrayList<TabletServerStatus> getTabletServers()  throws CBException, CBSecurityException, TableNotFoundException {
	MasterMonitorInfo mmi=null;  
	AuthInfo authInfo = authInfo();
	CloudbaseConnection connector = connection();

	MasterClientService.Client client = null;
	ArrayList<TabletServerStatus> tservers = new ArrayList<TabletServerStatus>();
	try {
	    client = MasterClient.getConnection(connector.getInstance());
	    mmi = client.getMasterStats(authInfo);
	    if (mmi != null)
		tservers.addAll(mmi.tServerInfo);

	} catch (Exception e) {
	    mmi = null;

	} finally {
	    if (client != null)
		MasterClient.close(client);
	}
	return tservers;
    }

    /*
     *
     *
     *
     */
    public long getNumberOfEntries(String tserverAddress, ArrayList<String>  tableNames) throws CBException, CBSecurityException, TableNotFoundException {
	long retValue=0l;
	TreeMap<String, TabletInfo> tabletMap =null;
	
	try {
	    
	    tabletMap = getTabletInfo(tserverAddress);
	    for (Entry<String, TabletInfo> te : tabletMap.entrySet()) {
		log.fine(" Entry key = "+ te.getKey());
		TabletInfo info = te.getValue();
		String thisTableName =null;
		
		if (te.getKey().isEmpty() ) {
		    continue;
		}
		String[] tabletIdent = te.getKey().split(";");
		try {

		    thisTableName = getTableName( tabletIdent[0]);//Tables.getTableName(connector.getInstance(), tabletIdent[0]);
		} catch (TableNotFoundException e) {
		    log.warning(tabletIdent[0]+ e);
		    continue;
		}
		log.fine("Table name ["+thisTableName + "] has  "+ info.numEntries+ "  entries.");
		for(String tableName: tableNames) {
		    if(tableName.equals(thisTableName) ) {
			retValue += info.numEntries;
		    }
		}
	    }
	    	    
	} catch (Exception e) {
	    Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.SEVERE,"",e);
	    return -1;
	} finally {    
	}

	return retValue;
	
    }
    /*  
     *  Get the total number of entries for the specified table names
     *  tableNames   list of table names of interest
     */
    public long getNumberOfEntries(ArrayList<String>  tableNames) throws CBException, CBSecurityException, TableNotFoundException {
	long retVal=0;

	ArrayList<TabletServerStatus> tservers = getTabletServers();
	for (TabletServerStatus status : tservers) {
	    Logger.getLogger(D4mDbTableOperations.class.getName()).log(Level.FINE,"TabletServer status ::  name = "+status.name);
	    retVal += getNumberOfEntries(status.name, tableNames);
	}

	return retVal;
    }
    /*
     *  Count the number of rows in this table of this cloud instance
     *
     */
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

    /*
     *  Count the total number of rows of all tables in this cloud instance
     *
     */
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

    public ArrayList<String> getSplits(String tableName) throws CBException, TableNotFoundException, CBSecurityException {
	ArrayList<String> list = new ArrayList<String>();
	CloudbaseConnection connector = connection();
	Collection<Text> splitsList = connector.getSplits(tableName);

	for(Text txt : splitsList) {
	    String s =new String( txt.getBytes());
	    list.add(s);
	}
	return list;
    }
    private CloudbaseConnection connection() throws CBException, TableNotFoundException, CBSecurityException {
	/*
	String user = this.connProps.getUser();
	byte[] pass = this.connProps.getPass().getBytes();
	String instanceName = this.connProps.getInstanceName();
	String zooKeepers = this.connProps.getHost();
	*/
	CloudbaseConnection connector = new CloudbaseConnection(this.connProps);
	return connector;
    }
    private AuthInfo authInfo() throws CBException, TableNotFoundException, CBSecurityException {
	String user = this.connProps.getUser();
	byte[] pass = this.connProps.getPass().getBytes();
	
	return 	 new AuthInfo(user,pass);

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

