
/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.accumulo;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.D4mException;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.SortedSet;

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
	private PasswordToken passwordToken;
	Credentials creds= null;
	/**
	 * 
	 */
	public AccumuloConnection(ConnectionProperties conn) throws AccumuloException,AccumuloSecurityException {
		this.conn = conn;
		ClientConfiguration cconfig = new ClientConfiguration().withInstance(conn.getInstanceName()).withZkHosts(conn.getHost()).withZkTimeout(conn.getSessionTimeOut());
		this.instance = new ZooKeeperInstance(cconfig);
		this.passwordToken = new PasswordToken(this.conn.getPass());
		this.creds = new Credentials(this.conn.getUser(), this.passwordToken);

        //principal = username = this.conn.getUser()
        System.out.println("about to make connector: user="+this.conn.getUser()+"   password="+ new String(this.passwordToken.getPassword()));
        this.connector = this.instance.getConnector(this.conn.getUser(), this.passwordToken);
        System.out.println("made connector");
        String [] sAuth = conn.getAuthorizations();
        if (sAuth != null && sAuth.length > 0) {
            this.auth = new Authorizations(sAuth);
        } else {
            this.auth= org.apache.accumulo.core.Constants.NO_AUTHS;
        }
        log.debug("!!!WHOAMI="+this.connector.whoami());
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

	public MasterClientService.Client getMasterClient() throws TTransportException {
		return MasterClient.getConnection(getInstance());
	}

	public TabletClientService.Iface getTabletClient (String tserverAddress) throws TTransportException {
        //HostAndPort address = AddressUtil.parseAddress(tserverAddress, -1);
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
		return this.connector.tableOperations().list();
	}

	// TODO these are just wrappers; why have them when we could expose the TableOperations object directly?
	public void addIterator(String tableName, IteratorSetting iterSet) throws D4mException
	{
		TableOperations tops = this.connector.tableOperations();
		try {
			tops.attachIterator(tableName, iterSet); // adds on all scopes: majc, minc, scan 
		} catch (AccumuloSecurityException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		} catch (AccumuloException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		} catch (TableNotFoundException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		}
	}

	public Map<String,EnumSet<IteratorUtil.IteratorScope>> listIterators(String tableName) throws D4mException
	{
		TableOperations tops = this.connector.tableOperations();
		try {
			return tops.listIterators(tableName);
		} catch (AccumuloSecurityException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		} catch (AccumuloException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		} catch (TableNotFoundException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		}
	}

	public IteratorSetting getIteratorSetting(String tableName, String name, IteratorUtil.IteratorScope scope) throws D4mException
	{
		TableOperations tops = this.connector.tableOperations();
		try {
			return tops.getIteratorSetting(tableName, name, scope);
		} catch (AccumuloSecurityException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		} catch (AccumuloException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		} catch (TableNotFoundException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		}
	}

	public void removeIterator(String tableName, String name, EnumSet<IteratorUtil.IteratorScope> scopes) throws D4mException
	{
		TableOperations tops = this.connector.tableOperations();
		try {
			tops.removeIterator(tableName, name, scopes);
		} catch (AccumuloSecurityException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		} catch (AccumuloException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		} catch (TableNotFoundException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		}
	}

	public void checkIteratorConflicts(String tableName, IteratorSetting cfg, EnumSet<IteratorScope> scopes) throws D4mException 
	{
		TableOperations tops = this.connector.tableOperations();
		try {
			tops.checkIteratorConflicts(tableName, cfg, scopes);
		} catch (AccumuloException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		} catch (TableNotFoundException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		}

	}

	public void merge(String tableName, String startRow, String endRow) throws D4mException {
		TableOperations tops = this.connector.tableOperations();
		try {
			tops.merge(tableName, startRow == null ? null : new Text(startRow), endRow == null ? null : new Text(endRow));
		} catch (AccumuloException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		} catch (TableNotFoundException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		} catch (AccumuloSecurityException e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		}
	}

	public  TCredentials getCredentials() throws D4mException {
		TCredentials tCred = null;
		try {
			tCred =  this.creds.toThrift(this.instance);  //.create(this.conn.getUser(), this.passwordToken, this.instance.getInstanceID() );
		} catch (Exception e) {
			log.warn(e);
			//e.printStackTrace();
			throw new D4mException(e);
		}
		return tCred;
	}

	public void setConnectionProperties(ConnectionProperties connProp) {
		this.conn = connProp;
	}
	/*
	 *   auths    comma-separated list of authorizations
	 */	
	public void setAuthorizations(String auths) {
		if(auths == null) {
			this.auth= org.apache.accumulo.core.Constants.NO_AUTHS;
			return;
		}
		this.auth = new Authorizations(auths.split(","));
	}
	public void setAuthorizations(ConnectionProperties connProp) {

		String [] sAuth = connProp.getAuthorizations();

		if(sAuth != null && sAuth.length > 0 )
			this.auth = new Authorizations(sAuth);
		else if( sAuth == null ){
			this.auth= org.apache.accumulo.core.Constants.NO_AUTHS;
		}
	}

	public String locateTablet(String tableName, String splitName) {
		String tabletName = null;
		TabletLocator tc = null;
		try {
			tc = TabletLocator.getLocator(this.instance, new Text(Tables.getTableId(this.instance, tableName))); // change to getLocator for 1.6
			
			org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation loc =
					tc.locateTablet(this.creds, new Text(splitName), false, false);
			tabletName = loc.tablet_location;
			log.debug("TableName="+tableName+", TABLET_NAME = "+tabletName);
		} catch (TableNotFoundException e) {
			log.warn(e);
			e.printStackTrace();
		} catch (AccumuloException e) {
			log.warn(e);
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			log.warn(e);
			e.printStackTrace();
		}


		return tabletName;
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
