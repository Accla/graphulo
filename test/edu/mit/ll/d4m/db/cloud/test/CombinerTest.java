package edu.mit.ll.d4m.db.cloud.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.mit.ll.d4m.db.cloud.accumulo.AccumuloConnection;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.D4mConfig;
import edu.mit.ll.d4m.db.cloud.D4mDataSearch;
import edu.mit.ll.d4m.db.cloud.D4mDbInsert;
import edu.mit.ll.d4m.db.cloud.D4mDbResultSet;
import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations;
import edu.mit.ll.d4m.db.cloud.D4mException;

public class CombinerTest {
	
	private String instanceName = "accumulo";
	private String host = "D4Muser.llgrid.ll.mit.edu:2181";
	private String username = "AccumuloUser";
	private String password = "9P20WV666KK119YY";
	
	private String tableName = "TestTableIterator";
	private String columnFamily="";
	private String columnVisibility="";
	private AccumuloConnection connection;
	
	@Before
	public void setUp() throws Exception {
		
		// Setup Connection
		D4mConfig.getInstance().setCloudType(D4mConfig.ACCUMULO);
		ConnectionProperties cp;
		cp = new ConnectionProperties();
		cp.setInstanceName(instanceName);
		cp.setHost(host);
		cp.setPass(password);
		cp.setUser(username);
		connection = new AccumuloConnection(cp);
		
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
	
	@Test
	public void testAddDataToSumCombineColumns() throws Exception {
		String iterName = "summingIter";
		
		// Setup IteratorSetting
		IteratorSetting cfg = new IteratorSetting(1, iterName, SummingCombiner.class);
		LongCombiner.setEncodingType(cfg, LongCombiner.Type.STRING);
		// add columns to act on
		List<IteratorSetting.Column> combineColumns = new ArrayList<IteratorSetting.Column>();
		combineColumns.add(new IteratorSetting.Column(columnFamily, "leg"));
		combineColumns.add(new IteratorSetting.Column(columnFamily, "pet"));
		Combiner.setColumns(cfg, combineColumns);
		
		// Add Iterator to table
		connection.addIterator(tableName, cfg);
		// Verify successful add
		Map<String,EnumSet<IteratorUtil.IteratorScope>> iterMap = connection.listIterators(tableName);
		EnumSet<IteratorUtil.IteratorScope> iterScope = iterMap.get(iterName);
		assertNotNull(iterScope);
		assertTrue(iterScope.containsAll(EnumSet.allOf(IteratorUtil.IteratorScope.class)));
		//IteratorSetting retSetting = connection.getIteratorSetting(table, iterName, IteratorUtil.IteratorScope.majc); // any scope ok
		//assertEquals(cfg, retSetting); // IteratorSetting has no equals method, but this will be true (verified manually)
		
		
		D4mDbInsert dbInsert = new D4mDbInsert(instanceName,host,tableName,username,password);
		D4mDataSearch dbQuery = new D4mDataSearch(instanceName, host, tableName, username, password);
		D4mDbResultSet dbResults;
		/*ArrayList<D4mDbRow> dbResultRows;*/
		
		// Initial insert before real insert
		String row = "id1,id1,id2,id2,";
		String col = "leg,pet,leg,pet,";
		String val = "1,1,1,1,";
		dbInsert.doProcessing(row, col, val,columnFamily,columnVisibility);
		// verify initial insert worked
		row = ":";
		col= ":";
		dbResults = dbQuery.doMatlabQuery(row, col, columnFamily, columnVisibility);
		//dbResultRows = dbResults.getMatlabDbRow();
		//dbQuery.next();
		System.out.println("After initial add:");
		System.out.println(dbQuery.getResults().prettyPrint());
		
		System.out.println("After adding (id1,leg,2)");
		row = "id1,";
		col = "leg,";
		val = "2,";
		dbInsert.doProcessing(row, col, val,columnFamily,columnVisibility);
		// verify
		row = ":";
		col= ":";
		dbResults = dbQuery.doMatlabQuery(row, col, columnFamily, columnVisibility);
		//dbResultRows = dbResults.getMatlabDbRow();
		System.out.println(dbQuery.getResults().prettyPrint());
		
		System.out.println("After adding (id1,leg,5), (id1,leg,1), (id1,pet,1), (id2,leg,12), (id2,pet,-1)");
		row = "id1,id1,id1,id2,id2,";
		col = "leg,leg,pet,leg,pet,";
		val = "5,1,1,12,-1,";
		dbInsert.doProcessing(row, col, val, columnFamily,columnVisibility);
		// verify
		row = ":";
		col= ":";
		dbResults = dbQuery.doMatlabQuery(row, col, columnFamily, columnVisibility);
		//dbResultRows = dbResults.getMatlabDbRow();
		System.out.println(dbQuery.getResults().prettyPrint());
		
		System.out.println("After removing iterator and adding (id2,leg,3), (id2,pet,19)");
		connection.removeIterator(tableName, iterName, EnumSet.allOf(IteratorUtil.IteratorScope.class));
		iterMap = connection.listIterators(tableName);
		assertFalse(iterMap.containsKey(iterName));
		// insert - should overwrite data now and not accumulate
		row = "id2,id2,";
		col = "leg,pet,";
		val = "3,19,";
		dbInsert.doProcessing(row, col, val, columnFamily,columnVisibility);
		// verify
		row = ":";
		col= ":";
		dbResults = dbQuery.doMatlabQuery(row, col, columnFamily, columnVisibility);
		//dbResultRows = dbResults.getMatlabDbRow();
		System.out.println(dbQuery.getResults().prettyPrint());
		
	}

	
	@Test
	public void testcolumnCombine() throws Exception
	{
		String s;
		D4mDbInsert dbInsert = new D4mDbInsert(instanceName,host,tableName,username,password);
		D4mDataSearch dbQuery = new D4mDataSearch(instanceName, host, tableName, username, password);
		D4mDbTableOperations dbTops = new edu.mit.ll.d4m.db.cloud.D4mDbTableOperations(instanceName, host, username, password, D4mConfig.ACCUMULO);
		
		System.out.println("Designating maxc1,maxc2 as max columns ---");
		dbTops.designateCombiningColumns(tableName, "maxc1,maxc2,", "max", columnFamily);
		System.out.println(dbTops.listCombiningColumns(tableName));
		
		System.out.println("Initial add ---");
		String row = "id1,id1,id2,id2,id1,id2,";
		String col = "maxc1,maxc2,maxc1,maxc2,sumc1,sumc1,";
		String val = "1,1,1,1,1,1,";
		dbInsert.doProcessing(row, col, val,columnFamily,columnVisibility);
		// verify initial insert worked
		row = ":";
		col= ":";
		dbQuery.doMatlabQuery(row, col, columnFamily, columnVisibility);
		System.out.println(s = dbQuery.getResults().prettyPrint());
		assertEquals(s, "id1 maxc1 1\nid1 maxc2 1\nid1 sumc1 1\nid2 maxc1 1\nid2 maxc2 1\nid2 sumc1 1\n");
		
		System.out.println("After adding (id1,maxc1,2), (id1,maxc2,-5), (id2,maxc1,4), (id1,sumc1,5)");
		row = "id1,id1,id2,id1,";
		col = "maxc1,maxc2,maxc1,sumc1,";
		val = "2,-5,4,5,";
		dbInsert.doProcessing(row, col, val,columnFamily,columnVisibility);
		// verify initial insert worked
		row = ":";
		col= ":";
		dbQuery.doMatlabQuery(row, col, columnFamily, columnVisibility);
		System.out.println(s = dbQuery.getResults().prettyPrint());
		assertEquals(s, "id1 maxc1 2\nid1 maxc2 1\nid1 sumc1 5\nid2 maxc1 4\nid2 maxc2 1\nid2 sumc1 1\n");
		
		System.out.println("Revoking maxc2 as max column; designating sumc1 as sum column");
		dbTops.revokeCombiningColumns(tableName, "maxc2;", columnFamily);		
		dbTops.designateCombiningColumns(tableName, "sumc1,", "sum", columnFamily);
		System.out.println(s = dbTops.listCombiningColumns(tableName));
		assertEquals(s, "SUM	:sumc1\nMAX	:maxc1\n");
		
		System.out.println("Adding (id1,maxc1,7), (id1,maxc2,0), (id2,maxc2,1), (id2,sumc1,2)");
		row = "id1,id1,id2,id2,";
		col = "maxc1,maxc2,maxc2,sumc1,";
		val = "7,0,1,2,";
		dbInsert.doProcessing(row, col, val,columnFamily,columnVisibility);
		// verify initial insert worked
		row = ":";
		col= ":";
		dbQuery.doMatlabQuery(row, col, columnFamily, columnVisibility);
		System.out.println(s = dbQuery.getResults().prettyPrint());
		assertEquals(s, "id1 maxc1 7\nid1 maxc2 0\nid1 sumc1 6\nid2 maxc1 4\nid2 maxc2 1\nid2 sumc1 3\n");
		
	}
	
	@Test //(expected=IllegalArgumentException.class)
	public void testMultipleItertatorsOnColumnIsOk() throws D4mException
	{
		D4mDbTableOperations dbTops = new edu.mit.ll.d4m.db.cloud.D4mDbTableOperations(instanceName, host, username, password, D4mConfig.ACCUMULO);
		dbTops.designateCombiningColumns(tableName, "maxc1,", "max", columnFamily);
		dbTops.designateCombiningColumns(tableName, "maxc1,", "max", columnFamily);
	}
	
	@Test
	public void testScientificNotationAndFloatsAreOk() throws D4mException
	{
		String s;
		D4mDbInsert dbInsert = new D4mDbInsert(instanceName,host,tableName,username,password);
		D4mDataSearch dbQuery = new D4mDataSearch(instanceName, host, tableName, username, password);
		D4mDbTableOperations dbTops = new edu.mit.ll.d4m.db.cloud.D4mDbTableOperations(instanceName, host, username, password, D4mConfig.ACCUMULO);
		
		System.out.println("Designating maxc1 as max column, sumc1 as sum column ---");
		dbTops.designateCombiningColumns(tableName, "sumc1,", "sum_decimal", columnFamily);
		dbTops.designateCombiningColumns(tableName, "maxc1,", "max_decimal", columnFamily);
		System.out.println(dbTops.listCombiningColumns(tableName));
		
		System.out.println("Initial add ---");
		String row = "id1,id1,id1,id1,";
		String col = "maxc1,sumc1,maxc1,sumc1,";
		String val = "1,1,2,2,";
		dbInsert.doProcessing(row, col, val,columnFamily,columnVisibility);
		row = ":";
		col= ":";
		dbQuery.doMatlabQuery(row, col, columnFamily, columnVisibility);
		System.out.println(s = dbQuery.getResults().prettyPrint());
		assertEquals(s, "id1 maxc1 2\nid1 sumc1 3\n");
		
		System.out.println("Adding 7.0000e+09 to both ---");
		row = "id1,id1,";
		col = "maxc1,sumc1,";
		val = "7.0000e+09,7.0000e+09,";
		dbInsert.doProcessing(row, col, val,columnFamily,columnVisibility);
		row = ":";
		col= ":";
		dbQuery.doMatlabQuery(row, col, columnFamily, columnVisibility);
		System.out.println(s = dbQuery.getResults().prettyPrint());
		assertEquals(s, "id1 maxc1 7.0000E+9\nid1 sumc1 7000000003\n");
		
		System.out.println("Adding 1.8500e+09 to both ---");
		row = "id1,id1,";
		col = "maxc1,sumc1,";
		val = "1.8500e+09,1.8500e+09,";
		dbInsert.doProcessing(row, col, val,columnFamily,columnVisibility);
		row = ":";
		col= ":";
		dbQuery.doMatlabQuery(row, col, columnFamily, columnVisibility);
		System.out.println(s = dbQuery.getResults().prettyPrint());
		assertEquals(s, "id1 maxc1 7.0000E+9\nid1 sumc1 8850000003\n");
	}
}
