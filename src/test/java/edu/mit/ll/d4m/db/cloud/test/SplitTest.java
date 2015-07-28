package edu.mit.ll.d4m.db.cloud.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.mit.ll.d4m.db.cloud.accumulo.AccumuloConnection;
import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.D4mDataSearch;
import edu.mit.ll.d4m.db.cloud.D4mDbInsert;
import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations;

public class SplitTest {
	private String instanceName;
	private String host;
	private String username;
    private String password;
	private String tableName = "TestTableRandom";
	private String columnFamily="fam";
	private String columnVisibility="";
	private AccumuloConnection connection;
	
	@Before
	public void setUp() throws Exception {
		
		// Setup Connection
        AccumuloTestConnection testConnection = new AccumuloTestConnection("CombinerTest.conf");
        ConnectionProperties cp = testConnection.getConnectionProperties();
        instanceName               = cp.getInstanceName();
        host                       = cp.getHost();
        username                   = cp.getUser();
        password                   = cp.getPass();
        connection = testConnection.getAccumuloConnection();
		
		// Create Table (delete if already existing)
		if (connection.tableExist(tableName))
			connection.deleteTable(tableName);
		connection.createTable(tableName);
		assumeTrue(connection.tableExist(tableName));
	}
	
	@After
	public void tearDown() throws Exception {
		D4mDbTableOperations dbTable = new D4mDbTableOperations(instanceName,host,username,password);
		dbTable.deleteTable(tableName);
	}
	
	/**
	 * Remember: after making a split, the Accumulo guarantees FUTURE added data will be split accordingly.  
	 * Previously added data may be kept on a different tablet than you expect.
	 * @throws Exception
	 */
	@Test
	public void doTest() throws Exception {
		String[] arr;
		D4mDbTableOperations dbTable = new D4mDbTableOperations(instanceName,host,username,password);
		D4mDbInsert dbInsert = new D4mDbInsert(instanceName, host, tableName, username, password);
		D4mDataSearch dbQuery = new D4mDataSearch(instanceName, host, tableName, username, password);
		
		String row = "aaaaid1,zid2,";
		String col = "leg,leg,";
		String val = "1,1,";
		dbInsert.doProcessing(row, col, val, columnFamily, columnVisibility); // last two are family and visibility
		
		row = ":";
		col= ":";
		dbQuery.doMatlabQuery(row, col, columnFamily, columnVisibility);
		System.out.println("After initial add:");
		System.out.println(dbQuery.getResults().prettyPrint());
		
		arr = dbTable.getSplits(tableName, true);
		System.out.printf("FIRST : %s\nSECOND: %s\n", arr[0], arr[1]);
		assertTrue(arr[0].isEmpty());
		//assertEquals(arr[1], "4,");
		
		dbTable.addSplits(tableName, "d;w;");
		
		arr = dbTable.getSplits(tableName, true);
		System.out.printf("FIRST : %s\nSECOND: %s\n", arr[0], arr[1]);
		assertEquals(arr[0], "d,w,");
		//assertEquals(arr[1], "0,1,1,")
		
		row = "aaaaid1,id2,";
		col = "pet,pet,";
		val = "1,1,";
		dbInsert.doProcessing(row, col, val, columnFamily, columnVisibility); // last two are family and visibility
		
		row = ":";
		col= ":";
		dbQuery.doMatlabQuery(row, col, columnFamily, columnVisibility);
		System.out.println("After second add:");
		System.out.println(dbQuery.getResults().prettyPrint());
		
		arr = dbTable.getSplits(tableName, true);
		System.out.printf("FIRST : %s\nSECOND: %s\n", arr[0], arr[1]);
		assertEquals(arr[0], "d,w,");
		//assertEquals(arr[1], "0,1,1,");
		
		dbTable.putSplits(tableName,  "g;w;y;");
		
		arr = dbTable.getSplits(tableName, true);
		System.out.printf("FIRST : %s\nSECOND: %s\n", arr[0], arr[1]);
		assertEquals(arr[0], "g,w,y,");
		//assertEquals(arr[1], "0,1,0,1,");
		
		dbTable.putSplits(tableName, "");
		
		arr = dbTable.getSplits(tableName, true);
		System.out.printf("FIRST : %s\nSECOND: %s\n", arr[0], arr[1]);
		assertTrue(arr[0].isEmpty());
		//assertEquals(arr[1], "4,");
	}
}
