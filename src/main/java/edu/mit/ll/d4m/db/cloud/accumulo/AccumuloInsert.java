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

/**
 * @author CHV8091
 *
 */
public class AccumuloInsert extends D4mInsertBase {
	private static Logger log = Logger.getLogger(AccumuloInsert.class);

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
			//if(D4mConfig.SORT_MUTATIONS){
			//	makeMutations();
			//	addMutations();
			//} else {
			makeAndAddMutations();
			//}
		} catch (MutationsRejectedException | TableNotFoundException e) {
			log.error(e);
            throw(e);
		}

  }

	private void makeAndAddMutations() throws TableNotFoundException, MutationsRejectedException {
		//		AccumuloConnection connection = new AccumuloConnection(super.connProps);
		BatchWriter bw = this.connection.createBatchWriter(tableName);
//		HashMap<String, Object> rowsMap = D4mQueryUtil.processParam(rows);
//		HashMap<String, Object> colsMap = D4mQueryUtil.processParam(cols);
//		HashMap<String, Object> weightMap = D4mQueryUtil.processParam(vals);

		String[] rowsArr = D4mQueryUtil.processParam(rows);//(String[]) rowsMap.get("content");
		String[] colsArr = D4mQueryUtil.processParam(cols);//(String[]) colsMap.get("content");
		String[] valsArr = D4mQueryUtil.processParam(vals);//(String[]) weightMap.get("content");

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
		bw.flush();
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
