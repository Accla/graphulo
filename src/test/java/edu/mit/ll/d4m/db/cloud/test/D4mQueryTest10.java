/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.test;

import java.util.ArrayList;
import java.util.HashMap;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.mit.ll.d4m.db.cloud.D4mDbInsert;
import edu.mit.ll.d4m.db.cloud.accumulo.D4mDbQueryAccumulo;
import edu.mit.ll.d4m.db.cloud.D4mDbResultSet;
import edu.mit.ll.d4m.db.cloud.D4mDbRow;
import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations;
import edu.mit.ll.d4m.db.cloud.D4mFactory;
import edu.mit.ll.d4m.db.cloud.util.D4mDataObj;

/**
 * @author CHV8091
 *
 */
public class D4mQueryTest10 {

	private static Logger log = Logger.getLogger(D4mQueryTest10.class);

	String instanceName = "";
	String host = "";
	String username = "";
	String password = "";
	String table = "";
	String columnFamily="";
	int i127=127;
	String ASCI_127 = new Character((char)i127).toString();

	@Before
	public void setUp() throws Exception {

		System.setProperty(D4mFactory.PROPERTY_D4M_CLOUD_TYPE,"Accumulo");
        AccumuloTestConnection testConnection = new AccumuloTestConnection("local.conf");
        ConnectionProperties cp = testConnection.getConnectionProperties();
        instanceName               = cp.getInstanceName();
        host                       = cp.getHost();
        username                   = cp.getUser();
        password                   = cp.getPass();
		table                      = "iTest10";
		String row = "a,a,a,a,a,a,aa,aa,aaa,aaa,b,b,bb,bb,bbb,bbb,";
		String col = "a,aa,aaa,b,bb,bbb,a,aa,a,aaa,a,b,a,bb,a,bbb,";
		String val = "a-a,a-aa,a-aaa,a-b,a-bb,a-bbb,aa-a,aa-aa,aaa-a,aaa-aaa,b-a,b-b,bb-a,bb-bb,bbb-a,bbb-bbb,";
	//	tearDown();
		D4mDbInsert dbInsert = new D4mDbInsert(instanceName,host,table,username,password);

		dbInsert.doProcessing(row, col, val,"","");


	}
	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		D4mDbTableOperations dbTable = new D4mDbTableOperations(instanceName,host,username,password);
		dbTable.deleteTable(table);

	}

	@Test
	public void test() {
		String rows="a,";
		String cols= ":";
		String authorizations="";
		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");

		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		
		try {
			//First query
			d4m.setLimit(10);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			d4m.next();
			print(d4m.getResults());

			rows="b,";
			cols= ":";
			//Second query
			System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			d4m.next();

			//d4m.doNewMatlabQuery(rows, cols, columnFamily, authorizations);
			//d4m.nextNewMatlabQuery();
			//D4mDbQueryTest.print(d4m);

		} catch (Exception e) {
			e.printStackTrace();
		} 
		finally {
			d4m.close();
		}
		System.out.println("--------------------------------------");
	}
	@Test
	public void test2() {
		String rows=":";
		String cols= ":";
		String authorizations="";
		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");

		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		try {
			//First query
			d4m.setLimit(0);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			d4m.next();
			print(d4m.getResults());


				//D4mDbQueryTest.print(d4m);

		} catch (Exception e) {
			e.printStackTrace();
		} 
		finally {
			d4m.close();
		}
		System.out.println("--------------------------------------");
	}

	@Test
	public void test3() {
		//String rows="b,:,b";
		String rows="a,";
		int i127 = 127;
		String ascii127 = new Character((char)i127).toString();
		//rows = rows+ascii127+",";
		String cols= "a,:,a";
		cols = cols+ascii127+",";
		String authorizations="";

		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		

		try {
			d4m.setLimit(1);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			
			while(d4m.hasNext()) {
				d4m.next();
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			HashMap<String,String> map = new HashMap<String,String>();
			String [] rowArray = cols.split(",");
			map.put(rowArray[0], rowArray[0]);
			//map.put(rowArray[1], rowArray[1]);
			
			for(D4mDbRow row : rowList) {
				String rowkey = row.getColumn();
				Assert.assertTrue(map.containsKey(rowkey));
			}
			D4mDbQueryTest.print(d4m);
			Assert.assertTrue(size == 6);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
			System.out.println("--------------------------------------");
		}

	}

	@Test
	public void test4() {
		//String rows="b,:,b";
		String rows="a,";
		int i127 = 127;
		String ascii127 = new Character((char)i127).toString();
		//rows = rows+ascii127+",";
		String cols= "a,:,aaa,";
		//cols = cols+ascii127+",";
		String authorizations="";

		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");
		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		

		try {
			d4m.setLimit(1);
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			
			while(d4m.hasNext()) {
				d4m.next();
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();

			HashMap<String,String> map = new HashMap<String,String>();
			String [] rowArray = cols.split(",");
			map.put(rowArray[0], rowArray[0]);
			//map.put(rowArray[1], rowArray[1]);
			
			for(D4mDbRow row : rowList) {
				String rowkey = row.getColumn();
				Assert.assertTrue(map.containsKey(rowkey));
			}
			D4mDbQueryTest.print(d4m);
			Assert.assertTrue(size == 6);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
			System.out.println("--------------------------------------");
		}

	}

	public void print(D4mDataObj d4mData) {
		System.out.println(d4mData.getRow());
		ArrayList<D4mDbRow>  testDataList = d4mData.getRowList();
		if(testDataList != null) {
			System.out.println("SIZE = "+testDataList.size());
			int cnt = 0;
			for(D4mDbRow row : testDataList) {
				cnt++;
				System.out.println(cnt+" :   "+row.toString());
			}
		}
	}
}
