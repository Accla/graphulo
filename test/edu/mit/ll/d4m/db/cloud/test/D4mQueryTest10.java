/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.test;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.TableNotFoundException;

import edu.mit.ll.d4m.db.cloud.D4mDbInsert;
import edu.mit.ll.d4m.db.cloud.D4mDbQuery;
import edu.mit.ll.d4m.db.cloud.D4mDbRow;
import edu.mit.ll.d4m.db.cloud.D4mQueryFactory;
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

		System.setProperty(D4mQueryFactory.PROPERTY_D4M_CLOUD_TYPE,"BigTableLike");
		instanceName               ="cloudbase";
		host                       = "f-2-6.llgrid.ll.mit.edu:2181";
		username                   =  "cbuser";
		password                   = "cbuser123";
		table                      = "iTest10";
		String row = "a,a,a,a,a,a,aa,aa,aaa,aaa,b,b,bb,bb,bbb,bbb,";
		String col = "a,aa,aaa,b,bb,bbb,a,aa,a,aaa,a,b,a,bb,a,bbb,";
		String val = "a-a,a-aa,a-aaa,a-b,a-bb,a-bbb,aa-a,aa-aa,aaa-a,aaa-aaa,b-a,b-b,bb-a,bb-bb,bbb-a,bbb-bbb,";
		D4mDbInsert dbInsert = new D4mDbInsert(instanceName,host,table,username,password);

		dbInsert.doProcessing(row, col, val,"","");


	}
	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		String rows="a,";
		String cols= ":";
		String authorizations="";
		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");

		D4mDbQuery d4m = new D4mDbQuery(instanceName, host, table, username, password);
		d4m.doTest = true;
		try {
			//First query
			d4m.setLimit(1);
			d4m.doNewMatlabQuery(rows, cols, columnFamily, authorizations);
			d4m.nextNewMatlabQuery();
			print(d4m.getD4m().getResults());

			rows="b,";
			cols= ":";
			//Second query
			System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");

			d4m.doNewMatlabQuery(rows, cols, columnFamily, authorizations);
			d4m.nextNewMatlabQuery();
			//D4mDbQueryTest.print(d4m);

		} catch (Exception e) {
			e.printStackTrace();
		} 
		finally {
			d4m.close();
		}
		System.out.println("--------------------------------------");
	}

	public void print(D4mDataObj d4mData) {
		System.out.println(d4mData.getRow());
		ArrayList<D4mDbRow>  testDataList = d4mData.getRowList();
		if(testDataList != null) {
			System.out.println("SIZE = "+testDataList.size());
			for(D4mDbRow row : testDataList) {
				System.out.println(row.toString());
			}
		}
	}
}
