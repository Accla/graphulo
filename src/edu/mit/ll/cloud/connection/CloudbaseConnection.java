package edu.mit.ll.cloud.connection;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import cloudbase.core.CBConstants;
import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.BatchWriter;
import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Connector;
import cloudbase.core.client.Instance;
import cloudbase.core.client.MasterInstance;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.TableExistsException;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.client.admin.TableOperations;
import cloudbase.core.data.Key;
import cloudbase.core.data.Range;
import cloudbase.core.security.Authorizations;
import cloudbase.core.security.SystemPermission;
import cloudbase.core.security.TablePermission;

/**
 * @author wi20909
 */

public class CloudbaseConnection {
	private static Logger log = Logger.getLogger(CloudbaseConnection.class);
	private Connector connector = null;
	private String tableName = "";
	private Authorizations authorizations = CBConstants.NO_AUTHS;
    //    private String instanceID=null;
    private int maxNumThreads = 1;
    
	public CloudbaseConnection(ConnectionProperties connProps) throws CBException, CBSecurityException {
		if (connProps.getInstanceName() == "" || connProps.getInstanceName() == null) {
			// Uses Cloudbase MasterInstance to connect.
			log.debug("Trying to connect to Master: " + connProps.getHost());
			MasterInstance instanceObj = new MasterInstance(connProps.getHost());
			this.connector = new Connector(instanceObj, connProps.getUser(), connProps.getPass().getBytes());
		}
		else {
			// Uses ZooKeeper host(s)... can be a comma delim list.
			ZooKeeperInstance instanceObj = new ZooKeeperInstance(connProps.getInstanceName(), connProps.getHost());
			//			this.connector = new Connector(instanceObj, connProps.getUser(), connProps.getPass().getBytes());
			this.connector = instanceObj.getConnector( connProps.getUser(), connProps.getPass().getBytes());
                     
		}
		this.maxNumThreads = connProps.getMaxNumThreads();
		String[] auths = connProps.getAuthorizations();
		if(auths != null && auths.length > 0 && !auths[0].equals(""))
			this.authorizations = new Authorizations(auths);
	}

	public void createTable(String tableName) throws CBException, CBSecurityException, TableExistsException {
		this.connector.tableOperations().create(tableName);
	}

    public void createTable(String tableName, String partitionKey) throws CBException, CBSecurityException, TableExistsException {
	//	Text text = new Text(partitionKey);
	//SortedSet<Text> sortedSet = new TreeSet<Text>();
	//sortedSet.add(text);
	// this.connector.tableOperations().create(tableName, sortedSet);
	List<String> list = new ArrayList<String>();
	list.add(partitionKey);
	createTable(tableName,list);
    }

    public void createTable(String tableName, List<String> partitionKeys) throws CBException, CBSecurityException, TableExistsException {

	SortedSet<Text> sortedSet = new TreeSet<Text>();
	for(String partitionKey:partitionKeys) {
	    Text text = new Text(partitionKey);
	    sortedSet.add(text);
	}
        this.connector.tableOperations().create(tableName, sortedSet);
    }

    /*
     *  SplitTable will "dynamically" split the table given a partitionKey
     *
     *   tableName   name of table to partition
     *   partitionKey   key to use for partition or a comma-separated list of keys
     */
    public void splitTable(String tableName, String partitionKey) throws CBException, CBSecurityException,TableNotFoundException {
	ArrayList<String> listOfPartitionKeys = new ArrayList<String>();

	String [] pKeysArray = partitionKey.split(",");
	for(int i = 0; i < pKeysArray.length; i++) {
	    String s = pKeysArray[i];
	    if(s.length() > 0)
		listOfPartitionKeys.add(s);	
	}
	splitTable(tableName, listOfPartitionKeys);
    }

    /*
     *  SplitTable will "dynamically" split the table based on a list of partition keys.
     *
     *   tableName  name of table
     *   partitionKeys   list of keys (string) for using to partition
     * 
     */
    public void splitTable(String tableName, List<String> partitionKeys) throws CBException, CBSecurityException, TableNotFoundException {

	SortedSet<Text> sortedSet = new TreeSet<Text>();
	for(String partitionKey:partitionKeys) {
	    Text text = new Text(partitionKey);
	    sortedSet.add(text);
	}
        this.connector.tableOperations().addSplits(tableName, sortedSet);
    }

    public void splitTable(String tableName, SortedSet<Text> partitionKeys) throws CBException, CBSecurityException, TableNotFoundException {

//    	SortedSet<Text> sortedSet = new TreeSet<Text>();
//    	for(String partitionKey:partitionKeys) {
//    	    Text text = new Text(partitionKey);
//    	    sortedSet.add(text);
//    	}
            this.connector.tableOperations().addSplits(tableName, partitionKeys);
        }

	public Scanner getScanner() throws TableNotFoundException, CBException, CBSecurityException {
		Scanner scanner = connector.createScanner(this.tableName, this.authorizations);
		return scanner;
	}

	public Scanner getScanner(String tableName) throws TableNotFoundException, CBException, CBSecurityException {
		Scanner scanner = connector.createScanner(tableName, this.authorizations);
		return scanner;
	}

	public BatchScanner getBatchScanner(int numberOfThreads) throws TableNotFoundException, CBException, CBSecurityException {
		BatchScanner scanner = connector.createBatchScanner(this.tableName, this.authorizations, numberOfThreads);
		return scanner;
	}

	public BatchScanner getBatchScanner(String tableName, int numberOfThreads) throws TableNotFoundException, CBException, CBSecurityException {
		BatchScanner scanner = connector.createBatchScanner(tableName, this.authorizations, numberOfThreads);
		return scanner;
	}

	public Scanner getRow(String tableName, String row) throws CBException, CBSecurityException, TableNotFoundException {
		// Create a scanner
		Scanner scanner = this.getScanner(tableName);
		Key key = new Key(new Text(row));
		// Say start key is the one with key of row
		// and end key is the one that immediately follows the row
		scanner.setRange(new Range(key, true, key.followingKey(1), false));
		return scanner;
	}

	public void deleteTable(String tableName) throws CBException, CBSecurityException, TableNotFoundException {

		this.connector.tableOperations().delete(tableName);

	}

	public boolean doesTableExist(String tableName) throws CBException, CBSecurityException, TableNotFoundException {

		if (this.connector.tableOperations().exists(tableName)) {
			return true;
		}
		else {
			return false;
		}
	}

	public SortedSet<String> getTableList() throws CBException, CBSecurityException {
		TableOperations ops = this.connector.tableOperations();
		SortedSet<String> set = ops.list();
		return set;
	}

	/**
	 * This was Depricated and was not changed in the migrsation to 1.1 from 1.0
	 * for d4M so I (William Smith 4/26/2010) changed to below. public
	 * BatchWriter getBatchWriter(String tableName) throws CBException,
	 * CBSecurityException, TableNotFoundException { BatchWriter bw =
	 * this.connector.createBatchWriter(tableName, 100000, 30, 1); return bw; }
	 */

	public BatchWriter getBatchWriter(String tableName) throws CBException, CBSecurityException, TableNotFoundException {
		BatchWriter bw = this.connector.createBatchWriter(tableName, Long.valueOf("100000"), Long.valueOf("30"), this.maxNumThreads);
		return bw;
	}

    /*
     * Return a ZookeeperInstance or MasterInstance of the cloud instance
     */
    public Instance getInstance() {
	return this.connector.getInstance();
    }
    public String getInstanceID() {
	Instance instance =getInstance();
	return instance.getInstanceID();
    }
    public TableOperations getTableOperations() {
	return this.connector.tableOperations();
    }

    public Collection<Text> getSplits(String tableName) throws TableNotFoundException {
    	Collection<Text> textColl= null;
    	
    	try {
			textColl=getTableOperations().getSplits(tableName);
		} catch (TableNotFoundException e) {
			log.warn("Cannot find table "+tableName);
		}
    	
    	return textColl;
    }
    
    /*
     * Grant system permission to the user
     */
    public void grantSystemPermission(String user, SystemPermission permission) throws CBException, CBSecurityException {
    	this.connector.securityOperations().grantSystemPermission(user, permission);

    }
    
    /*
     * Add a new user.
     * You must be root to add a new user
     * 
     *   user   The name of the user
     *   password  The user's password
     *   authorizations  the user's authorizations
     */
    public void addUser(String user, String password, String [] authorizations) throws CBException, CBSecurityException {
  
    	Authorizations auth = CBConstants.NO_AUTHS;
    	if(authorizations != null)
    		auth = new Authorizations(authorizations);
    	this.connector.securityOperations().createUser(user, password.getBytes(),auth );
    }
    
    /*
     * Grant table permission to the user
     */
    public void grantTablePermission(String user, String tableName, TablePermission permission) throws CBException, CBSecurityException {   	
    	this.connector.securityOperations().grantTablePermission(user, tableName, permission);
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
