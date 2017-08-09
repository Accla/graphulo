/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.accumulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import edu.mit.ll.d4m.db.cloud.D4mInsertBase;
import edu.mit.ll.d4m.db.cloud.util.D4mQueryUtil;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author CHV8091
 *
 */
public class AccumuloInsert extends D4mInsertBase {
	private static final Logger log = Logger.getLogger(AccumuloInsert.class);

	AccumuloConnection connection=null;
	public AccumuloInsert() {
		super();
	}


	public AccumuloInsert(String instanceName, String hostName,
			String tableName, String username, String password) {
		super(instanceName, hostName, tableName, username, password);

	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mInsertBase#doProcessing()
	 */
	@Override
	public void doProcessing() throws AccumuloException,AccumuloSecurityException,TableNotFoundException{
		if(connection == null)
			connection = new AccumuloConnection(super.connProps);

		//Create table
		createTable();
		//Build mutations and sort
		//Add mutation
		//    make connection
		try {
			makeAndAddMutations();
		} catch (MutationsRejectedException | TableNotFoundException e) {
			log.error(e);
            throw e;
		}

  }

  private static class RowIndex implements Comparator<Integer> {
		private final String[] array;

		public RowIndex(String[] array) {
			this.array = array;
		}

		public Integer[] createIndexArray() {
			Integer[] indexes = new Integer[array.length];
			for (int i = 0; i < array.length; i++) {
				indexes[i] = i;
			}
			return indexes;
		}

		@Override
		public int compare(Integer index1, Integer index2) {
			return array[index1].compareTo(array[index2]);
		}
	}

	private void makeAndAddMutations() throws TableNotFoundException, MutationsRejectedException {
		//		AccumuloConnection connection = new AccumuloConnection(super.connProps);
//		HashMap<String, Object> rowsMap = D4mQueryUtil.processParam(rows);
//		HashMap<String, Object> colsMap = D4mQueryUtil.processParam(cols);
//		HashMap<String, Object> weightMap = D4mQueryUtil.processParam(vals);

		final String[] rowsArr = D4mQueryUtil.processParam(rows);//(String[]) rowsMap.get("content");
		if (rowsArr.length == 0)
			return;
		final String[] colsArr = D4mQueryUtil.processParam(cols);//(String[]) colsMap.get("content");
		final String[] valsArr = D4mQueryUtil.processParam(vals);//(String[]) weightMap.get("content");

		final BatchWriter bw = this.connection.createBatchWriter(tableName);

		// DH: group same rows together for efficient mutations
		final RowIndex rowIndex = new RowIndex(rowsArr);
		final Integer[] indices = rowIndex.createIndexArray();
		Arrays.sort(indices, rowIndex);

		final ColumnVisibility colVisibility = new ColumnVisibility(super.visibility);
		final Text colFamily = new Text(super.family);
		String mRow = rowsArr[indices[0]];
		Mutation m = new Mutation(mRow);
		final Text mCol = new Text(colsArr[indices[0]]);
		final Value mVal = new Value(valsArr[indices[0]]);

		for (int i : indices) {
			if (!mRow.equals(rowsArr[i])) {
				bw.addMutation(m);
				mRow = rowsArr[i];
				m = new Mutation(rowsArr[i]);
			}
			mCol.set(colsArr[i]);
			mVal.set(valsArr[i].getBytes(UTF_8));

			if (log.isDebugEnabled())
				log.debug(i+" - INSERTING [r,c,v] =  ["+ mRow +","+mCol+","+mVal+"]");

			m.put(colFamily, mCol, colVisibility, mVal);
		}

		bw.addMutation(m);
		bw.close();
	}

	private void createTable () throws AccumuloException,AccumuloSecurityException{
		if(connection == null)
			connection = new AccumuloConnection(super.connProps);
		if(!connection.tableExist(super.tableName)) {
			connection.createTable(super.tableName);
		}
	}
}
