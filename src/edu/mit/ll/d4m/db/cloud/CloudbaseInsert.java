/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import java.util.HashMap;

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
import edu.mit.ll.d4m.db.cloud.util.MutationSorter;

/**
 * @author CHV8091
 *
 */
public class CloudbaseInsert extends D4mInsertBase {
	private static Logger log = Logger.getLogger(CloudbaseInsert.class);
	String[] rowsArr=null;

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
		try {
			this.createTable();
			makeMutations();
			addMutations();
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

		//Sort the mutations

		//Add mutations
	}
	
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
		catch (CBException ex) {
			log.warn(ex);
		}
		catch (CBSecurityException ex) {
			log.warn("Security problem", ex);
		}
		return exist;
	}

	private void makeMutations() {
		HashMap<String, Object> rowsMap = D4mQueryUtil.processParam(rows);
		HashMap<String, Object> colsMap = D4mQueryUtil.processParam(cols);
		HashMap<String, Object> weightMap = D4mQueryUtil.processParam(vals);

		rowsArr = (String[]) rowsMap.get("content");
		String[] colsArr = (String[]) colsMap.get("content");
		String[] valsArr = (String[]) weightMap.get("content");
		ColumnVisibility colVisibility = new ColumnVisibility(super.visibility);
		Text colfamily = new Text(super.family);

		for (int i = 0; i < rowsArr.length; i++) {

			String thisRow = rowsArr[i];
			String thisCol = colsArr[i];
			String thisVal = valsArr[i];
			Mutation m=null;
			Text column = new Text(thisCol);

			Value value = new Value(thisVal.getBytes());

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
		CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);
		BatchWriter batchWriter = cbConnection.getBatchWriter(tableName);

		for (int i = 0; i < rowsArr.length; i++) {

			String thisRow = rowsArr[i];
			Mutation m = (Mutation)mutSorter.get(thisRow);
			batchWriter.addMutation(m);
		}
	}
}
