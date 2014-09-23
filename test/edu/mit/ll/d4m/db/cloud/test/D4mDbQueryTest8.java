package edu.mit.ll.d4m.db.cloud.test;


import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.mit.ll.d4m.db.cloud.accumulo.D4mDbQueryAccumulo;
/**
 * @author cyee
 *
 */
public class D4mDbQueryTest8 {
	private static Logger log = Logger.getLogger(D4mDbQueryTest8.class);

	String instanceName = "";
	String host = "";
	String username = "";
	String password = "";
	String table = "";
	String columnFamily="";
	int i127=127;
	String ASCI_127 = new Character((char)i127).toString();


	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		instanceName               ="accumulo";
		host                       = "f-2-6.llgrid.ll.mit.edu:2181";
		username                   =  "AccumuloUser";
		password                   = "";
		table                      = "YAHOO_STANFORD_TEMPORALROW_NORMAL_TRANS";
	}
	@After
	public void tearDown() throws Exception {


	}

	@Test
	public void test1() {

		String rows=":";
		String cols= "20110320,:,20110601,";
		String authorizations="";
		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");

		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		d4m.setLimit(20);
		long start = System.currentTimeMillis();
		try {
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			long end = System.currentTimeMillis();
			double elapsed = ((double)(end - start))/1000.0;
			log.info("Query time (sec) = "+elapsed);                
			D4mDbQueryTest.print(d4m);
		}
		catch(Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	public void test2() {

		String rows=":";
		String cols= ":";
		String authorizations="";
		System.out.println("QUERY = ['"+ rows + "', '"+cols+"']");

		D4mDbQueryAccumulo d4m = new D4mDbQueryAccumulo(instanceName, host, table, username, password);
		d4m.doTest = true;
		d4m.setLimit(100);
		long start = System.currentTimeMillis();
		try {
			d4m.doMatlabQuery(rows, cols, columnFamily, authorizations);
			long end = System.currentTimeMillis();
			double elapsed = ((double)(end - start))/1000.0;
			log.info("Query time (sec) = "+elapsed);                
			D4mDbQueryTest.print(d4m);
		}
		catch(Exception e) {
			e.printStackTrace();
		}

	}

}
