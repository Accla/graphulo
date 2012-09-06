/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import cloudbase.core.client.BatchWriter;
import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.TableExistsException;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.data.Mutation;
import cloudbase.core.data.Value;
import cloudbase.core.security.ColumnVisibility;
import edu.mit.ll.cloud.connection.CloudbaseConnection;
import edu.mit.ll.d4m.db.cloud.util.D4mQueryUtil;

/**
 * @author CHV8091
 *
 */
public class CloudbaseInsert extends D4mInsertBase {
	private static Logger log = Logger.getLogger(CloudbaseInsert.class);
	String[] rowsArr=null;
	CloudbaseConnection cbConnection=null;
	public CloudbaseInsert() {
		super();
	}
	/**
	 * @param instanceName
	 * @param hostName
	 * @param tableName
	 * @param username
	 * @param password
	 */
	public CloudbaseInsert(String instanceName, String hostName,
			String tableName, String username, String password) {
		super(instanceName, hostName, tableName, username, password);

	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mInsertBase#doProcessing()
	 */
	@Override
	public void doProcessing()  {
		// TODO Auto-generated method stub
		//	Create the table
		long start = System.currentTimeMillis();
		this.cbConnection = CloudbaseConnection.getConnectionInstance(super.connProps);
		try {
			this.createTable();
			//if(D4mConfig.SORT_MUTATIONS) {

			//make the mutations, sorting
			//	makeMutations();
			// each mutation is sent to the cloud with the batchwriter
			//	addMutations();
			//	} else {
			//Make mutation and write it via BatchWriter
			makeAndAddMutation();
			//}
		} catch (CBException e) {
			e.printStackTrace();
		} catch (CBSecurityException e) {
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		log.debug("Insert elapsed time (sec) = "+((double)(end-start))/1000.0);

	}

	public void createTable() throws CBException, CBSecurityException {

		//		if (this.doesTableExistFromMetadata(tableName) == false) {
		try {
			if(this.cbConnection == null)
				cbConnection = CloudbaseConnection.getConnectionInstance(super.connProps);//new CloudbaseConnection(super.connProps);
			if(!cbConnection.doesTableExist(tableName)) {
				cbConnection.createTable(tableName);
			}

		}
		catch (TableExistsException ex) {
			System.out.println("Table already exists.");
		}
		//		}
		catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public boolean doesTableExistFromMetadata(String tableName) {
		boolean exist = false;
		D4mDbInfo info = new D4mDbInfo(super.connProps);
		D4mConfig d4mConfig = D4mConfig.getInstance();
		String cloudType = d4mConfig.getCloudType();
		info.setCloudType(cloudType);
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

	/*
	 *  The rows,cols, and values are sorted.
	 *  Entries with the same row id are put in the same Mutation object.
	 */
	private void makeMutations() {
//		HashMap<String, Object> rowsMap = D4mQueryUtil.processParam(rows);
//		HashMap<String, Object> colsMap = D4mQueryUtil.processParam(cols);
//		HashMap<String, Object> weightMap = D4mQueryUtil.processParam(vals);

		rowsArr = D4mQueryUtil.processParam(rows);//(String[]) rowsMap.get("content");
		String[] colsArr = D4mQueryUtil.processParam(cols);//(String[]) colsMap.get("content");
		String[] valsArr = D4mQueryUtil.processParam(vals);//(String[]) weightMap.get("content");
		ColumnVisibility colVisibility = new ColumnVisibility(super.visibility);
		Text colfamily = new Text(super.family);

		for (int i = 0; i < rowsArr.length; i++) {

			String thisRow = rowsArr[i].trim();
			String thisCol = colsArr[i].trim();
			String thisVal = valsArr[i];
			Mutation m=null;
			Text column = new Text(thisCol);

			Value value = new Value(thisVal.getBytes());
			// Sort the mutation by rowid
			if(!mutSorter.hasMutation(thisRow)) {
				m = new Mutation(new Text(thisRow));
				mutSorter.add(thisRow, m);
			} else {
				m = (Mutation)mutSorter.get(thisRow);
			}
			m.put(colfamily, column, colVisibility, value);

		}
	}

	private void addMutations() throws CBException, CBSecurityException, TableNotFoundException {
		if(this.cbConnection == null)
			cbConnection = CloudbaseConnection.getConnectionInstance(super.connProps);// new CloudbaseConnection(super.connProps);

		BatchWriter batchWriter = cbConnection.getBatchWriter(tableName);

		for (int i = 0; i < rowsArr.length; i++) {

			String thisRow = rowsArr[i].trim();
			Mutation m = (Mutation)mutSorter.get(thisRow);
			batchWriter.addMutation(m);
		}
		batchWriter.flush();
		batchWriter.close();
	}

	private void makeAndAddMutation() throws CBException, CBSecurityException, TableNotFoundException {
		//if(this.cbConnection == null)
			//cbConnection = new CloudbaseConnection(super.connProps);
		BatchWriter batchWriter = this.cbConnection.getBatchWriter(tableName);

//		HashMap<String, Object> rowsMap = D4mQueryUtil.processParam(rows);
//		HashMap<String, Object> colsMap = D4mQueryUtil.processParam(cols);
//		HashMap<String, Object> weightMap = D4mQueryUtil.processParam(vals);

		rowsArr = D4mQueryUtil.processParam(rows);//(String[]) rowsMap.get("content");
		String[] colsArr = D4mQueryUtil.processParam(cols);//(String[]) colsMap.get("content");
		String[] valsArr = D4mQueryUtil.processParam(vals);//(String[]) weightMap.get("content");
		ColumnVisibility colVisibility = new ColumnVisibility(super.visibility);
		Text colfamily = new Text(super.family);

		for (int i = 0; i < rowsArr.length; i++) {

			String thisRow = rowsArr[i].trim();
			String thisCol = colsArr[i].trim();
			String thisVal = valsArr[i];
			Mutation m=null;
			Text column = new Text(thisCol);

			Value value = new Value(thisVal.getBytes());

			//if(!mutSorter.hasMutation(thisRow)) {
			m = new Mutation(new Text(thisRow));

			m.put(colfamily, column, colVisibility, value);
			batchWriter.addMutation(m);


		}
		batchWriter.flush();
		batchWriter.close();

	}
}
