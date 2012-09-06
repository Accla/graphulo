/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Instance;
import cloudbase.core.client.TableExistsException;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.client.impl.MasterClient;
import cloudbase.core.client.impl.Tables;
import cloudbase.core.conf.CBConfiguration;
import cloudbase.core.master.thrift.MasterClientService;
import cloudbase.core.master.thrift.MasterMonitorInfo;
import cloudbase.core.master.thrift.TabletServerStatus;
import cloudbase.core.security.thrift.AuthInfo;
import cloudbase.core.tabletserver.thrift.TabletClientService;
import cloudbase.core.tabletserver.thrift.TabletStats;
import cloudbase.core.util.AddressUtil;
import cloudbase.core.util.ThriftUtil;
import edu.mit.ll.cloud.connection.CloudbaseConnection;
import edu.mit.ll.cloud.connection.ConnectionProperties;

/**
 * Table operations for Cloudbase
 *  1. setup connection properties ConnectionProperties
 *  2. execute connect()
 *  3. do your operation
 *  
 * @author cyee
 *
 */
public class CloudbaseTableOperations implements D4mTableOpsIF {
	private static Logger log = Logger.getLogger(CloudbaseTableOperations.class);

	private ConnectionProperties connProp=null;
	CloudbaseConnection cbConnection = null;

	/**
	 * 
	 */
	public CloudbaseTableOperations() {
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#createTable(java.lang.String)
	 */
	@Override
	public void createTable(String tableName) {


		try {
			cbConnection.createTable(tableName);
			System.out.println("The " + tableName + " table was created.");
		}
		catch (CBException ex) {
			log.warn(ex);
		}
		catch (CBSecurityException ex) {
			log.warn(ex);
		}
		catch (TableExistsException ex) {
			System.out.println("The " + tableName + " table already Exists.");
		}


	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#deleteTable(java.lang.String)
	 */
	@Override
	public void deleteTable(String tableName) {
		try {
			cbConnection.deleteTable(tableName);
			System.out.println("The " + tableName + " table was deleted.");
		}
		catch (CBException ex) {
			log.warn(ex);
		}
		catch (CBSecurityException ex) {
			log.warn(ex);
		}
		catch (TableNotFoundException ex) {
			log.warn(ex);
		}


	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#splitTable(java.lang.String, java.lang.String)
	 */
	@Override
	public void splitTable(String tableName, String partitions) {
		String [] pKeys = partitions.split(",");
		//System.out.println(" *** Number of partition keys = "+ pKeys.length);
		splitTable(tableName,pKeys);
	}
	/*
	 *  tableName  name of table to split
	 *  partitionKeys  array of strings
	 */
	public void splitTable(String tableName, String [] partitionKeys)   {
		ArrayList<String> list = new ArrayList<String>();
		for(int i =0; i < partitionKeys.length; i++) {
			list.add(partitionKeys[i]);
		}
		try {
			splitTable(tableName, list);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CBSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/*
	 *  tableName  name of table to split
	 *   partitionKeys   - list of keys (eg.  java.util.ArrayList)
	 */
	public void splitTable(String tableName, List<String> partitionKeys) throws IOException, CBException, CBSecurityException, TableNotFoundException {

		//CloudbaseConnection  cbConnection = new CloudbaseConnection(this.connProp);
		cbConnection.splitTable(tableName, partitionKeys);
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#getNumberOfEntries(java.util.ArrayList)
	 */
	@Override
	public long getNumberOfEntries(ArrayList<String> tableNames) {
		long retVal=0;

		ArrayList<TabletServerStatus> tservers =null;
		try {
			tservers = getTabletServers();
			for (TabletServerStatus status : tservers) {
				retVal += getNumberOfEntries(status.name, tableNames);
			}
		} catch (TTransportException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CBSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		return retVal;
	}
	@Override
	public List<org.apache.accumulo.core.tabletserver.thrift.TabletStats> getTabletStatsForTables(
			List<String> tableNames) {
		throw new UnsupportedOperationException("not implemented on Cloudbase");
	}

	@Override
	public void setConnProps(ConnectionProperties connProp) {
		this.connProp = connProp;
	}

	private ArrayList<TabletServerStatus> getTabletServers()  throws CBException, CBSecurityException, TableNotFoundException,TTransportException {
		MasterMonitorInfo mmi=null;  
		AuthInfo authInfo = authInfo();
		CloudbaseConnection connector = this.cbConnection;

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

	public long getNumberOfEntries(String tserverAddress, ArrayList<String>  tableNames) throws CBException, CBSecurityException, TableNotFoundException {
		long retval =0l;
		List<TabletStats> tsStats = getTabletStatsList(tserverAddress, tableNames);
		for(TabletStats info : tsStats) {
			retval += info.numEntries;
		}

		return retval;
	}

	private AuthInfo authInfo() throws CBException, TableNotFoundException, CBSecurityException {
		String user = this.connProp.getUser();
		byte[] pass = this.connProp.getPass().getBytes();

		return 	 new AuthInfo(user,pass, cbConnection.getInstanceID());

	}
	public List<TabletStats> getTabletStatsList(String tserverAddress, List<String> tableNamesList) throws CBException, CBSecurityException, TableNotFoundException  {
		InetSocketAddress address = AddressUtil.parseAddress(tserverAddress, -1);
		List<TabletStats> tsStats = new ArrayList<TabletStats>();
		Instance instance = cbConnection.getInstance();

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
			log.warn( e.toString());

		}

		return tsStats;
	}

	@Override
	public void connect() {
		if(cbConnection == null) {
			try {
				this.cbConnection = new CloudbaseConnection(connProp);
			} catch (CBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (CBSecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	@Override
	public void setConnProps(String instanceName, String host,
			String username, String password) {

		this.connProp = new ConnectionProperties();
		this.connProp.setHost(host);
		this.connProp.setInstanceName(instanceName);
		this.connProp.setUser(username);
		this.connProp.setPass(password);


	}

	@Override
	public void splitTable(String tableName, SortedSet<Text> partitions) {
		try {
			this.cbConnection.splitTable(tableName, partitions);
		} catch (CBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CBSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public List<String> getSplits(String tableName) {
		List<String> list = new ArrayList<String>();
		try {
			Collection<Text> coll = this.cbConnection.getSplits(tableName);
			
			for(Text t : coll) {
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
	public void addIterator(String tableName, IteratorSetting cfg) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Cloudbase iterators not supported (yet?)");
	}

	@Override
	public Map<String, EnumSet<IteratorScope>> listIterators(String tableName) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Cloudbase iterators not supported (yet?)");
	}

	@Override
	public IteratorSetting getIteratorSetting(String tableName,
			String iterName, IteratorScope scan) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Cloudbase iterators not supported (yet?)");
	}

	@Override
	public void removeIterator(String tableName, String name,
			EnumSet<IteratorScope> allOf) {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Cloudbase iterators not supported (yet?)");
	}

	@Override
	public void checkIteratorConflicts(String tableName, IteratorSetting cfg,
			EnumSet<IteratorScope> allOf) throws D4mException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Cloudbase iterators not supported (yet?)");
	}

	@Override
	public void merge(String tableName, String startRow, String endRow)
			throws D4mException {
		throw new UnsupportedOperationException("Cloudbase merge not supported");
		
	}

	

	/*@Override
	public void addSplits(String tableName, SortedSet<Text> splitsSet)
			throws D4mException {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException("Cloudbase addSplits not supported");
	}*/

}
/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% D4M: Dynamic Distributed Dimensional Data Model
% MIT Lincoln Laboratory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% (c) <2010> Massachusetts Institute of Technology
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 */
