/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import edu.mit.ll.cloud.connection.AccumuloConnection;
import edu.mit.ll.cloud.connection.ConnectionProperties;

/**
 * @author cyee
 *
 */
public class AccumuloTableOperations implements D4mTableOpsIF {
	private static Logger log = Logger.getLogger(AccumuloTableOperations.class);

	AccumuloConnection connection= null;
	ConnectionProperties connProp= null;

	/**
	 * 
	 */
	public AccumuloTableOperations() {

	}
	public AccumuloTableOperations(ConnectionProperties connProp) {
		this.connProp = connProp;
		connect();
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#createTable(java.lang.String)
	 */
	@Override
	public void createTable(String tableName) {
		this.connection.createTable(tableName);
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#deleteTable(java.lang.String)
	 */
	@Override
	public void deleteTable(String tableName) {
		this.connection.deleteTable(tableName);
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#splitTable(java.lang.String, java.lang.String)
	 */
	@Override
	public void splitTable(String tableName, String partitions) {
		String [] pKeys = partitions.split(",");
		//Make SortedSet
		TreeSet<Text> set = new TreeSet<Text>();

		for(String pt : pKeys) {
			Text text = new Text(pt);
			set.add(text);
		}
		this.connection.addSplit(tableName, set);

	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#getNumberOfEntries(java.util.ArrayList)
	 */
	@Override
	public long getNumberOfEntries(ArrayList<String> tableNames) {
		long retval=0l;

		//Get TServers
		try {
			ArrayList<TabletServerStatus> tserverStatusList = getTabletServers();
			List<TabletStats> tabletStatsList = getTabletStatsList(tserverStatusList,  tableNames);
			retval = getNumberOfEntries(tabletStatsList);
		} catch (ThriftSecurityException e) {
			log.warn(e);
		} catch (TException e) {
			log.warn(e);
		}
		
		// Connect to each tserver and get numEntries from each tableName
		//    Get the TabletStat

		return retval;
	}
	private long getNumberOfEntries(List<TabletStats> list) {
		long retval = 0;
		for(TabletStats ts: list) {
			log.debug("num entries = "+ts.numEntries);
			
			retval += ts.numEntries;
		}
		
		return retval;
	}
	
	/**
	 * Intended to be used for a single table.  Not for public use.
	 * @param tableNames
	 * @return
	 */
	@Override
	public List<TabletStats> getTabletStatsForTables(List<String> tableNames) {
		List<TabletStats> retval = null;

		//Get TServers
		try {
			ArrayList<TabletServerStatus> tserverStatusList = getTabletServers();
			retval = getTabletStatsList(tserverStatusList,  tableNames);
		} catch (ThriftSecurityException e) {
			log.warn(e);
		} catch (TException e) {
			log.warn(e);
		}
		
		// Connect to each tserver and get numEntries from each tableName
		//    Get the TabletStat

		return retval;
	}

	private ArrayList<TabletServerStatus> getTabletServers() throws ThriftSecurityException, TException {
		ArrayList<TabletServerStatus> list = new ArrayList<TabletServerStatus>();// list of TServer info
		MasterClientService.Iface client=null;
		try {
			MasterMonitorInfo mmi=null; 
			client = this.connection.getMasterClient();
			//changed in accumulo-1.4
			mmi = client.getMasterStats(null, getAuthInfo());

			list.addAll(mmi.getTServerInfo());
		} finally {
			ThriftUtil.returnClient(client);
		}
		return list;
	}
	private List<TabletStats> getTabletStatsList(List<TabletServerStatus> tserverNames, List<String> tableNames) {
		List<TabletStats> tabStatsList=new ArrayList<TabletStats>();
		int cnt=0;
		for(TabletServerStatus tss: tserverNames) {
			cnt++;
			String tserverName = tss.name;
			log.debug("["+cnt+"] - Tserver name = "+tserverName);
			List<TabletStats> tlist = getTabletStatsList(tserverName, tableNames);
			tabStatsList.addAll(tlist);
		}
		return tabStatsList;
	}
	/*
	 * Get numEntries from tserver
	 */
	private List<TabletStats> getTabletStatsList(String tserverName, List<String> tableNames) {
		MasterClientService.Iface masterClient= null;
		TabletClientService.Iface tabClient = null;
		AuthInfo authInfo  = getAuthInfo();
		List<TabletStats> tabStatsList = new ArrayList<TabletStats>();
		try {
			masterClient = this.connection.getMasterClient();
			tabClient = this.connection.getTabletClient(tserverName);
			Map<String, String> nameToIdMap = this.connection.getNameToIdMap();
			
			for(String tableName : tableNames) {
				
				String tableId = nameToIdMap.get(tableName);
				log.debug(tserverName+"-Tablet INFO ("+tableName+","+tableId+")");
				tabStatsList.addAll(tabClient.getTabletStats(null, authInfo, tableId));
			}
			
		} catch (TTransportException e) {
			log.warn(e);
		} catch (ThriftSecurityException e) {
			log.warn(e);
		} catch (TException e) {
			log.warn(e);
		} finally {
			ThriftUtil.returnClient(masterClient);
			ThriftUtil.returnClient(tabClient);
		}

		
		return tabStatsList;
	}
	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#setConnProps(edu.mit.ll.cloud.connection.ConnectionProperties)
	 */
	@Override
	public void setConnProps(ConnectionProperties connProp) {
		this.connProp = connProp;

	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#setConnProps(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void setConnProps(String instanceName, String host, String username,
			String password) {
		this.connProp = new ConnectionProperties();
		this.connProp.setHost(host);
		this.connProp.setInstanceName(instanceName);
		this.connProp.setUser(username);
		this.connProp.setPass(password);
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#connect()
	 */
	@Override
	public void connect() {
		this.connection = new AccumuloConnection(connProp);
	}
	public AuthInfo getAuthInfo() {
		String user = this.connProp.getUser();
		byte [] pw = this.connProp.getPass().getBytes();
		String instanceId = this.connection.getInstance().getInstanceID();
		//Accumulo-1.4 use ByteBuffer for the password in AuthInfo constructor
		ByteBuffer pwbuffer = ByteBuffer.wrap(pw);
		AuthInfo authinfo=new AuthInfo(user, pwbuffer, instanceId);
		return authinfo;
	}
	/*
	 *    private AuthInfo authInfo() throws CBException, TableNotFoundException, CBSecurityException {
	String user = this.connProps.getUser();
	byte[] pass = this.connProps.getPass().getBytes();
	CloudbaseConnection connector = connection();
	return 	 new AuthInfo(user,pass, connector.getInstanceID());

    }

	 */

	@Override
	public void splitTable(String tableName, String[] partitions) {
		TreeSet<Text> tset = new TreeSet<Text>();
		for(String pt : partitions) {
			tset.add(new Text(pt));
		}
		
		splitTable(tableName,tset);
	}

	@Override
	public void splitTable(String tableName, SortedSet<Text> partitions) {
		// TODO Auto-generated method stub
		this.connection.addSplit(tableName, partitions);

	}

	@Override
	public List<String> getSplits(String tableName) {
		Collection<Text> splitsColl=null;
		List<String> list = new ArrayList<String>();
		try {
			splitsColl = this.connection.getSplits(tableName);
			for(Text t: splitsColl) {
				String s = t.toString();
				list.add(s);
			}
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return list;
	}
	
	@Override
	public void addIterator(String tableName, IteratorSetting cfg) throws D4mException {
		this.connection.addIterator(tableName, cfg);
	}
	@Override
	public Map<String, EnumSet<IteratorScope>> listIterators(String tableName) throws D4mException {
		return this.connection.listIterators(tableName);
	}
	@Override
	public IteratorSetting getIteratorSetting(String tableName,
			String iterName, IteratorScope scope) throws D4mException {
		return this.connection.getIteratorSetting(tableName, iterName, scope);
	}
	@Override
	public void removeIterator(String tableName, String name,
			EnumSet<IteratorScope> scopes) throws D4mException {
		this.connection.removeIterator(tableName, name, scopes);
	}
	@Override
	public void checkIteratorConflicts(String tableName, IteratorSetting cfg,
			EnumSet<IteratorScope> scopes) throws D4mException {
		this.connection.checkIteratorConflicts(tableName, cfg, scopes);
		
	}
	/*@Override
	public void addSplits(String tableName, SortedSet<Text> splitsSet) throws D4mException {
		this.connection.addSplit(tableName, splitsSet);
		
	}*/
	@Override
	public void merge(String tableName, String startRow, String endRow) throws D4mException {
		this.connection.merge(tableName, startRow, endRow);
	}

}
