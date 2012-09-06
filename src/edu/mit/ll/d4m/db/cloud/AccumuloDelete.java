package edu.mit.ll.d4m.db.cloud;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.io.Text;

import edu.mit.ll.cloud.connection.AccumuloConnection;
import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.util.D4mQueryUtil;

/**
 * Used to delete triples in an Accumulo database given pairings of rows and columns.
 * @author dy23798
 *
 */
public class AccumuloDelete {
	
	private ConnectionProperties connectProps;
	private AccumuloConnection connection;
	
	public AccumuloDelete(String instanceName, String host, String user, String pass) {
		connectProps = new ConnectionProperties(host, user, pass, instanceName, null); // let null inputs go
		connection = new AccumuloConnection(connectProps);
	}
	
	public void deleteRows(String tableName, String rowString, String colString, String colFamilyString, String visibilityString) throws TableNotFoundException, MutationsRejectedException
	{
		if (tableName == null || rowString == null || colString == null)
			throw new IllegalArgumentException("null input");
		BatchWriter bw = connection.createBatchWriter(tableName);
		String[] rowsArr, colsArr;
		rowsArr = D4mQueryUtil.processParam(rowString);
		colsArr = D4mQueryUtil.processParam(colString);
		if (rowsArr.length != colsArr.length)
			throw new IllegalArgumentException("rows and column input does not matter");
		ColumnVisibility colVisibility = visibilityString == null ? new ColumnVisibility() : new ColumnVisibility(visibilityString);
		Text colFamily = colFamilyString == null ? new Text("") : new Text(colFamilyString);
		for(int i =0; i < rowsArr.length; i++) {
			Mutation m = new Mutation(new Text(rowsArr[i]));
			//m.put(colFamily, column, colVisibility, value);
			m.putDelete(colFamily, new Text(colsArr[i]), colVisibility);
			
			bw.addMutation(m);
			
		}
		bw.flush();
		bw.close();
		
	}
	
}
