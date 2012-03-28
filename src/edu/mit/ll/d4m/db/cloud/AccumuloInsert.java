/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import java.util.HashMap;
import org.apache.hadoop.io.Text;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;

import org.apache.log4j.Logger;

import edu.mit.ll.cloud.connection.AccumuloConnection;
import edu.mit.ll.d4m.db.cloud.util.D4mQueryUtil;

/**
 * @author CHV8091
 *
 */
public class AccumuloInsert extends D4mInsertBase {
	private static Logger log = Logger.getLogger(AccumuloInsert.class);

	String[] rowsArr=null;

	public AccumuloInsert() {
		super();
	}
	/**
	 * @param instanceName
	 * @param hostName
	 * @param tableName
	 * @param username
	 * @param password
	 */
	public AccumuloInsert(String instanceName, String hostName,
			String tableName, String username, String password) {
		super(instanceName, hostName, tableName, username, password);

	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mInsertBase#doProcessing()
	 */
	@Override
	public void doProcessing() {
		//Create table
		createTable();
		//Build mutations and sort
		//Add mutation
		//    make connection
		try {
			if(D4mConfig.SORT_MUTATIONS){
				makeMutations();
				addMutations();
			} else {
				makeAndAddMutations();
			}
		} catch (MutationsRejectedException e) {
			log.warn(e);
		} catch (TableNotFoundException e) {
			log.warn(e);
		}

	}

	private void makeMutations ( ) {
		HashMap<String, Object> rowsMap = D4mQueryUtil.processParam(rows);
		HashMap<String, Object> colsMap = D4mQueryUtil.processParam(cols);
		HashMap<String, Object> weightMap = D4mQueryUtil.processParam(vals);

		rowsArr = (String[]) rowsMap.get("content");
		String[] colsArr = (String[]) colsMap.get("content");
		String[] valsArr = (String[]) weightMap.get("content");

		ColumnVisibility colVisibility = new ColumnVisibility(super.visibility);
		Text colFamily = new Text(super.family);
		for(int i =0; i < rowsArr.length; i++) {
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
			m.put(colFamily, column, colVisibility, value);


		}
	}

	private void makeAndAddMutations() throws TableNotFoundException, MutationsRejectedException {
		AccumuloConnection connection = new AccumuloConnection(super.connProps);
		BatchWriter bw = connection.createBatchWriter(tableName, AccumuloConnection.maxMemory, AccumuloConnection.maxLatency, super.connProps.getMaxNumThreads());
		HashMap<String, Object> rowsMap = D4mQueryUtil.processParam(rows);
		HashMap<String, Object> colsMap = D4mQueryUtil.processParam(cols);
		HashMap<String, Object> weightMap = D4mQueryUtil.processParam(vals);

		rowsArr = (String[]) rowsMap.get("content");
		String[] colsArr = (String[]) colsMap.get("content");
		String[] valsArr = (String[]) weightMap.get("content");

		ColumnVisibility colVisibility = new ColumnVisibility(super.visibility);
		Text colFamily = new Text(super.family);
		for(int i =0; i < rowsArr.length; i++) {
			String thisRow = rowsArr[i];
			String thisCol = colsArr[i];
			String thisVal = valsArr[i];
			Mutation m=null;
			Text column = new Text(thisCol);

			Value value = new Value(thisVal.getBytes());
			log.debug(i+" - INSERTING [r,c,v] =  ["+ thisRow+","+thisCol+","+thisVal+"]");
			m = new Mutation(new Text(thisRow));
			m.put(colFamily, column, colVisibility, value);
			bw.addMutation(m);

		}


	}
	private void addMutations() throws TableNotFoundException, MutationsRejectedException {
		AccumuloConnection connection = new AccumuloConnection(super.connProps);
		BatchWriter bw = connection.createBatchWriter(tableName, AccumuloConnection.maxMemory, AccumuloConnection.maxLatency, super.connProps.getMaxNumThreads());
		for(int i = 0; i < rowsArr.length; i++) {
			String thisRow = rowsArr[i];
			log.debug(i+" - INSERTING row = "+ thisRow);
			Mutation m = (Mutation)mutSorter.get(thisRow);
			bw.addMutation(m);
		}
	}
	private void createTable () {
		AccumuloConnection connection = new AccumuloConnection(super.connProps);
		if(!connection.tableExist(super.tableName)) {
			connection.createTable(super.tableName);
		}
	}
}
