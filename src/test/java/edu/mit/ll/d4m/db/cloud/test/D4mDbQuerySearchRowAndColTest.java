/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.test;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.mit.ll.d4m.db.cloud.D4mDbInsert;
import edu.mit.ll.d4m.db.cloud.accumulo.D4mDbQueryAccumulo;
import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations;

/**
 * test the searchRowAndColumn
 * @author cyee
 *
 */
public class D4mDbQuerySearchRowAndColTest {
	String instanceName = "";
	String host = "";
	String username = "";
	String password = "";
	String table = "";
	String columnFamily="";

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		instanceName               ="accumulo";
		host                       = "f-2-6.llgrid.ll.mit.edu:2181";
		username                   =  "AccumuloUser";
		password                   = "";
		table                      = "iTest8";
		columnFamily        = "";
		
		/*
		 * 
		 * 
a :a []    a-a
a :aa []    a-aa
a :aaa []    a-aaa
a :b []    a-b
a :bb []    a-bb
a :bbb []    a-bbb
aa :a []    aa-a
aa :aa []    aa-aa
aaa :a []    aaa-a
aaa :aaa []    aaa-aaa
b :a []    b-a
b :b []    b-b
bb :a []    bb-a
bb :bb []    bb-bb
bbb :a []    bbb-a
bbb :bbb []    bbb-bbb

		 */

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
		D4mDbTableOperations dbTable = new D4mDbTableOperations(instanceName,host,username,password);
		dbTable.deleteTable(table);

	}

	/*
	 *  row = "a,b,";
	 *  col = ":";
	 */
	@Test
	public void test() {
		String rows="a b ";
		String cols= "a ";
		String authorizations="";

		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		try {
			//First query
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			D4mDbQueryTest.print(d4m);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally {
			d4m.close();
		}

	}
}
