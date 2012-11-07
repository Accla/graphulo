package edu.mit.ll.d4m.db.cloud;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

//import cloudbase.core.client.BatchWriter;
//import cloudbase.core.client.CBException;
//import cloudbase.core.client.CBSecurityException;
//import cloudbase.core.client.MutationsRejectedException;
//import cloudbase.core.client.TableExistsException;
//import cloudbase.core.client.TableNotFoundException;
//import cloudbase.core.data.Mutation;
//import cloudbase.core.data.Value;
//import cloudbase.core.security.ColumnVisibility;
import edu.mit.ll.cloud.connection.ConnectionProperties;
//import edu.mit.ll.d4m.db.cloud.cb.CloudbaseConnection;

/**
 * @author William Smith
 */

public class D4mDbInsert extends D4mParent {

	private static Logger log = Logger.getLogger(D4mDbInsert.class);
	private String tableName = "";
	private String rows = "";
	private String cols = "";
	private String vals = "";
	private String family = "";
	private String visibility = "";
	static final boolean doTest = false;
	static final boolean printOutput = false;
	static final int maxMutationsToCache = 10000;
	private int numThreads = 50;

	private ConnectionProperties connProps = new ConnectionProperties();

	private D4mInsertBase d4mInserter=null;
	/**
	 * Constructor that may use MasterInstance or ZooKeeperInstance to connect
	 * to CB.
	 * 
	 * @param connProps
	 * @param tableName
	 * @param rows
	 * @param cols
	 * @param vals
	 * @throws CBException
	 * @throws CBSecurityException
	 * @throws TableExistsException
	 */
	/*
	@Deprecated
	public D4mDbInsert(ConnectionProperties connProps, String tableName, String rows, String cols, String vals) throws CBException, CBSecurityException, TableExistsException {
		super();
		this.tableName = tableName;
		this.rows = rows;
		this.cols = cols;
		this.vals = vals;

		this.connProps = connProps;
	}
	*/

	/**
	 * @param instanceName
	 * @param hostName
	 * @param username
	 * @param password
	 * @param tableName
	 * @param rows
	 * @param cols
	 * @param vals
	 * @throws CBException
	 * @throws CBSecurityException
	 * @throws TableExistsException
	 */
	/*
	@Deprecated
	public D4mDbInsert(String instanceName, String hostName, String tableName, String username, String password, String rows, String cols, String vals) throws CBException, CBSecurityException, TableExistsException {
		super();
		this.tableName = tableName;
		this.rows = rows;
		this.cols = cols;
		this.vals = vals;

		this.connProps.setHost(hostName);
		this.connProps.setInstanceName(instanceName);
		this.connProps.setUser(username);
		this.connProps.setPass(password);
	}
*/
	/**
	 * @param instanceName
	 * @param hostName
	 * @param tableName
	 * @param username
	 * @param password
	 * @throws CBException
	 * @throws CBSecurityException
	 * @throws TableExistsException
	 */
	public D4mDbInsert(String instanceName, String hostName, String tableName, String username, String password)  {
		super();
		this.tableName = tableName;

		this.connProps.setHost(hostName);
		this.connProps.setInstanceName(instanceName);
		this.connProps.setUser(username);
		this.connProps.setPass(password);
		this.connProps.setMaxNumThreads(this.numThreads);
	}
	/**
	 * @param instanceName cloudbase instance
	 * @param hostName    zookeeper host
	 * @param tableName   name of table to insert data into
	 * @param username    user name
	 * @param password    user's password
	 * @param numThreads   number of threads to use to write to table
	 * @throws CBException
	 * @throws CBSecurityException
	 * @throws TableExistsException
	 */
	public D4mDbInsert(String instanceName, String hostName, String tableName, String username, String password, int numThreads) {
		super();
		this.tableName = tableName;

		this.connProps.setHost(hostName);
		this.connProps.setInstanceName(instanceName);
		this.connProps.setUser(username);
		this.connProps.setPass(password);
		this.numThreads= numThreads;
		this.connProps.setMaxNumThreads(this.numThreads);
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 5) {
			return;
		}

		String hostName = args[0];
		String tableName = args[1];
		String rows = args[2];
		String cols = args[3];
		String vals = args[4];
/*
		D4mDbInsert ci = new D4mDbInsert("cloudbase", hostName, tableName, "root", "secret", rows, cols, vals);
		String visibility = "s|ts|sci";
		String family = "d4mFamily";
		String columnQualifier = "d4mFamilyValue";
		ci.doProcessing(visibility, family, columnQualifier);
		*/
	}
	
	/*
	 * Package visibility because this is just for debugging for now
	 * Only use for adding rows with blank column visibility and blank column family
	 * @param d4mRows
	 /
	void doProcessing(List<D4mDbRow> d4mRows)
	{
		String rows="", cols="", vals="", family="", visibility="";
		boolean first = true;
		for (D4mDbRow d4mRow : d4mRows) {
			if (!first) {
				family = d4mRow.columnFamily;
				visibility = d4mRow.
			}
		}
	}*/

	public void doProcessing(String rows, String cols, String vals, String family, String visibility) throws D4mException {
		//throws IOException, CBException, CBSecurityException, TableNotFoundException, MutationsRejectedException {
		this.rows = rows;
		this.cols = cols;
		this.vals = vals;
		this.family = family;
		this.visibility = visibility;
		if(this.d4mInserter == null) {
			this.d4mInserter = D4mFactory.createInserter();
			this.d4mInserter.setConnProps(connProps);
		}
		this.d4mInserter.setTableName(this.tableName);

		long start = System.currentTimeMillis();
		this.d4mInserter.doProcessing(rows, cols, vals, family, visibility);
		long end = System.currentTimeMillis();
		double elapsed = ((double)(end-start))/1000.0;
		log.info("INGEST time elapsed(sec) = "+elapsed);
		System.out.println("INGEST time (sec) = "+elapsed);

		//
		//doProcessing();
	}

	/*
	//Insert using Cloudbase
	private void doProcessing() throws IOException, CBException, CBSecurityException, TableNotFoundException, MutationsRejectedException {

		// this.doLoadTest();
		this.createTable();
		Date startDate = new Date();
		long start = System.currentTimeMillis();

		CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);
		BatchWriter batchWriter = cbConnection.getBatchWriter(tableName);

		HashMap<String, Object> rowsMap = this.processParam(rows);
		HashMap<String, Object> colsMap = this.processParam(cols);
		HashMap<String, Object> weightMap = this.processParam(vals);

		String[] rowsArr = (String[]) rowsMap.get("content");
		String[] colsArr = (String[]) colsMap.get("content");
		String[] valsArr = (String[]) weightMap.get("content");

		for (int i = 0; i < rowsArr.length; i++) {

			String thisRow = rowsArr[i];
			String thisCol = colsArr[i];
			String thisVal = valsArr[i];

			Text family = new Text(this.family);
			Text column = new Text(thisCol);
			ColumnVisibility visibilitiy = new ColumnVisibility(this.visibility);
			Value value = new Value(thisVal.getBytes());

			Mutation m = new Mutation(new Text(thisRow));
			m.put(family, column, visibilitiy, value);
			batchWriter.addMutation(m);
			m = null;
		}
		batchWriter.close();

		double elapsed = (System.currentTimeMillis() - start);
		Date endDate = new Date();
		long endSeconds = System.currentTimeMillis();
		System.out.println("Time = " + elapsed / 1000 + "," + start / 1000 + "," + endSeconds / 1000 + "," + startDate + "," + endDate);
	}
	*/

	private HashMap<String, Object> processParam(String param) {
		HashMap<String, Object> map = new HashMap<String, Object>();
		String content = param.substring(0, param.length() - 1);
		String delimiter = param.replace(content, "");
		map.put("delimiter", delimiter);
		if (delimiter.equals("|")) {
			delimiter = "\\" + delimiter;
		}
		map.put("content", content.split(delimiter));
		map.put("length", content.length());
		return map;
	}
/*
	public void createTable() throws CBException, CBSecurityException {

		if (this.doesTableExistFromMetadata(tableName) == false) {
			try {
				CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);
				cbConnection.createTable(tableName);
			}
			catch (TableExistsException ex) {
				System.out.println("Table already exists.");
			}
		}
	}
*/
	public boolean doesTableExistFromMetadata(String tableName) {
		boolean exist = false;
		D4mDbInfo info = new D4mDbInfo(this.connProps);
		String tableNames = "";
		try {
			tableNames = info.getTableList();
			if (tableNames.contains(tableName)) {
				exist = true;
			}

		}
		catch (Exception ex) {
			log.warn(ex);
		}
		return exist;
	}

	public void doLoadTest() {
		int loops = 10;
		int capacity = loops;

		StringBuilder sb1 = new StringBuilder(capacity);
		StringBuilder sb2 = new StringBuilder(capacity);
		StringBuilder sb3 = new StringBuilder(capacity);

		System.out.println("Creating test data for " + loops + " entries.");
		for (int i = 1; i < loops + 1; i++) {
			sb1.append(i + " ");
			sb2.append(i + " ");
			sb3.append(i + " ");
		}

		this.rows = sb1.toString();
		this.cols = sb2.toString();
		this.vals = sb3.toString();
		System.out.println("Completed creation of test data for " + loops + " entries.");
	}

	/*
	@Deprecated
	public void doProcessing(String rows, String cols, String vals) throws IOException, CBException, CBSecurityException, TableNotFoundException, MutationsRejectedException {
		this.rows = rows;
		this.cols = cols;
		this.vals = vals;
		doProcessing();

	}
	*/
	/*
	 * Set cloud type - Cloudbase or Accumulo
	 */
	// 	public void setCloudType(String cloudType) {
	// 		D4mConfig d4mConf = D4mConfig.getInstance();
	// 		d4mConf.setCloudType(cloudType);
	// 	}

	/*
	 * partitionKey a string or comma-separated list
	 */
	/*
	 * public void splitTable(String partitionKey) throws IOException,
	 * CBException, CBSecurityException, TableNotFoundException { String []
	 * pKeys = partitionKey.split(",");
	 * //System.out.println(" *** Number of partition keys = "+ pKeys.length);
	 * splitTable(pKeys); }
	 */
	/*
	 * partitionKeys array of strings
	 */
	/*
	 * public void splitTable(String [] partitionKeys) throws IOException,
	 * CBException, CBSecurityException, TableNotFoundException {
	 * ArrayList<String> list = new ArrayList<String>(); for(int i =0; i <
	 * partitionKeys.length; i++) { list.add(partitionKeys[i]); }
	 * splitTable(list); }
	 */

	/*
	 * partitionKeys - list of keys (eg. java.util.ArrayList)
	 */
	/*
	 * public void splitTable(List<String> partitionKeys) throws IOException,
	 * CBException, CBSecurityException, TableNotFoundException {
	 * CloudbaseConnection cbConnection = new
	 * CloudbaseConnection(this.connProps);
	 * cbConnection.splitTable(this.tableName, partitionKeys); }
	 */
}
/*
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % D4M: Dynamic Distributed Dimensional Data Model 
 * % MIT Lincoln Laboratory
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % (c) <2010> Massachusetts Institute of Technology
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 */
