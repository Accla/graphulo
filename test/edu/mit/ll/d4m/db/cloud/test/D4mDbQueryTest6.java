/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.test;


import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Properties;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.TableNotFoundException;
import edu.mit.ll.d4m.db.cloud.D4mDbQuery;
import edu.mit.ll.d4m.db.cloud.D4mDbResultSet;
import edu.mit.ll.d4m.db.cloud.D4mDbRow;

/**
 * @author cyee
 *
 */
public class D4mDbQueryTest6 {
	String instanceName = "";
	String host = "";
	String username = "";
	String password = "";
	String table = "";
	String columnFamily="";
	static int count=0;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		//Load parameters from a config file
		Properties prop = new Properties();
		ClassLoader cloader = ClassLoader.getSystemClassLoader();
		URL url = cloader.getResource("test_config.properties");

		System.out.println("*****  FILE = "+url.getFile()+"  **********");
		try {
			InputStream ins = url.openStream();
			prop.load(ins);
			ins.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}


		instanceName               = prop.getProperty("cb.instance","cloudbase");
		host                       = prop.getProperty("cb.host","bullet:2181");
		username                   = prop.getProperty("cb.user", "root");
		password                   = prop.getProperty("cb.passwd", "secret");
		table                      = prop.getProperty("cb.table.name", "test_table");
		columnFamily        = prop.getProperty("cb.col.family", "");


	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	

	@Test	
	public void test6() {
		System.out.println("**************** ^^^^^^^^^^^^^ ***********************");
		System.out.println("**************** START  TEST 6 ***********************");


		//		String rows="row_0000000000,";
		String rows="row_000000000*,";
		String cols=":";
		String family= this.columnFamily;
		String authorizations="";
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		D4mDbQuery d4m = new D4mDbQuery(instanceName, host, table, username, password);
		int limit =100;
		int reps=2;

		try {
			d4m.setLimit(limit);
			d4m.doTest = true;
			d4m.doMatlabQuery(rows, cols, family, authorizations);
			System.out.println(d4m.rowReturnString+","+d4m.columnReturnString);
			for(int i=0;i< reps;i++) {

				d4m.next();
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();
			//System.out.println(d4m.rowReturnString+","+d4m.columnReturnString);

			D4mDbQueryTest.print(d4m);

			System.out.println("5 >>>> SIZE = "+size+"  <<<<");
			Assert.assertTrue(size == (limit+(limit*reps)));
			Thread.sleep(5000);
		} catch (CBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CBSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


			d4m.close();
			System.out.println("**************** END  TEST 6 ***********************");
			System.out.println("**************** %%%%%%%%%%% ***********************");

		}

	}


}
