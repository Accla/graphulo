package edu.mit.ll.d4m.db.cloud;

import java.util.ArrayList;
import java.util.Iterator;

import edu.mit.ll.d4m.db.cloud.D4mDbInsert;
import edu.mit.ll.d4m.db.cloud.D4mDbQuery;
import edu.mit.ll.d4m.db.cloud.D4mDbResultSet;
import edu.mit.ll.d4m.db.cloud.D4mDbRow;
import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations;

public class TestCRUD {

	public static void main(String[] args) throws Exception {

		String instanceName = args[0];
		String host = args[1];
		String username = args[2];
		String password = args[3];
		String tableName = args[4];
		instanceName = "";

		// create D4m Objects
		D4mDbTableOperations tblOps = new D4mDbTableOperations(instanceName, host, username, password);
		D4mDbInsert dbIn = new D4mDbInsert(instanceName, host, tableName, username, password);
		D4mDbQuery dbQuery = new D4mDbQuery(instanceName, host, tableName, username, password);

		// create table
		tblOps.createTable(tableName);

		// insert data
		dbIn.doProcessing("R1,R2,R3,R4,", "C1,C2,C3,C4,", "V1,V2,V3,V4,", "colFam", "");
		dbIn.doProcessing("RA,RB,RC,RD,", "CA,CB,CC,CD,", "VA,VB,VC,VD,", "colFam", "");
		dbIn.doProcessing("RA1,RB2,RC3,RD4,", "CA1,CB2,CC3,CD4,", "VA1,VB2,VC3,VD4,", "colFam", "");

		try {
			// query data
			D4mDbResultSet resultSet = dbQuery.doMatlabQuery(":", ":", "colFam", "");

			double totalQueryTime = resultSet.getQueryTime();
			System.out.println("totalQueryTime = " + totalQueryTime);
			ArrayList<?> rows = resultSet.getMatlabDbRow();

			Iterator<?> it = rows.iterator();
			System.out.println("");
			System.out.println("");

			int rowsToPrint = 20 + 1;
			int counter = 0;
			while (it.hasNext()) {
				counter++;
				D4mDbRow row = (D4mDbRow) it.next();
				String rowNumber = row.getRow();
				String column = row.getColumn();
				String value = row.getValue();
				String modified = row.getModified();
				if (counter < rowsToPrint) {
					System.out.println("Row; " + rowNumber);
					System.out.println("Column; " + column);
					System.out.println("Value; " + value);
					System.out.println("Modified; " + modified);
					System.out.println("");
					System.out.println("");
				}
			}

			System.out.println("RowReturnString=" + dbQuery.getRowReturnString());
			System.out.println("ColumnReturnString=" + dbQuery.getColumnReturnString());
			System.out.println("ValueReturnString=" + dbQuery.getValueReturnString());
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			// delete table
			tblOps.deleteTable(tableName);
		}
	}
}
