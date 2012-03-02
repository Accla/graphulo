/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.test;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Properties;

import org.junit.Test;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.After;

import cloudbase.core.client.BatchWriter;
import cloudbase.core.client.Connector;
import cloudbase.core.client.ZooKeeperInstance;
import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.TableExistsException;
import cloudbase.core.client.TableNotFoundException;
import edu.mit.ll.d4m.db.cloud.CbDataLoader;
import edu.mit.ll.d4m.db.cloud.D4mDbQuery;
import edu.mit.ll.d4m.db.cloud.D4mDbResultSet;
import edu.mit.ll.d4m.db.cloud.D4mDbRow;


/**
 * @author cyee
 *
 */
public class D4mDbQueryTest {

	String instanceName = "";
	String host = "";
	String username = "";
	String password = "";
	String table = "";
	String columnFamily="";
	static int count=0;
	//D4mDbQuery d4m = null;//new D4mDbQuery(instanceName, host, table, username, password);

	/**
	 * 
	 */
	public D4mDbQueryTest() {
		// TODO Auto-generated constructor stub
	}

	/*
	 *  Setup table with data 
	 *     c1 c2 c3 c4 c5 ...
	 *  r1 v1 v2 v3 v4 v5 ...
	 *  r2
	 *  r3
	 *  r4
	 *  r5
	 *  r6
	 */
	@Before
	public void setup() {
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
		columnFamily         = prop.getProperty("cb.col.family", "");
		String numEntries    = prop.getProperty("cb.num.entries");
		String numColEntries = prop.getProperty("cb.num.col.entries","1");
		String numThreads    = prop.getProperty("cb.num.threads","5");
		String visibility    = prop.getProperty("cb.col.vis","");;

		CbDataLoader cbdata = new CbDataLoader(username,
				password,
				instanceName,
				host,
				table,
				numEntries,
				numColEntries,
				columnFamily,
				numThreads,
				visibility);
		cbdata.run();

	}

	private void makeFakeData() {

	}

	/*
	 *  test1   get all data
	 */
	@Test
	public void test1() {
		System.out.println("**************** START  TEST 1 ***********************");

		//this.d4m.reset();
		D4mDbQuery d4m = new D4mDbQuery(instanceName, host, table, username, password);
		String rows=":";
		String cols = ":";
		String family= this.columnFamily;
		String authorizations="";
		try {
			d4m.setLimit(5);
			d4m.doTest = true;
			d4m.doMatlabQuery(rows, cols, family, authorizations);
			d4m.next();
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();
			System.out.println(d4m.rowReturnString);
			System.out.println(">>>> SIZE = "+size+"  <<<<");
			Assert.assertTrue(size == 10);
		} catch (CBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (CBSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		}
		finally
		{
			d4m.close();
			System.out.println("**************** END  TEST 1 ***********************"+"\n\n");

		}

	}

	@Test
	public void test2() {
		System.out.println("**************** START  TEST 2 ***********************");

		//
		String rows="row_0000000000,";
		//		String rows="row_0000000000,row_0000000001,";
		String cols=":";
		String family= this.columnFamily;
		String authorizations="";
		//d4m.reset();
		D4mDbQuery d4m = new D4mDbQuery(instanceName, host, table, username, password);
		int limit =5;
		int reps=2;
		try {
			d4m.setLimit(limit);
			d4m.doTest = true;
			d4m.doMatlabQuery(rows, cols, family, authorizations);
			System.out.println(d4m.rowReturnString+","+d4m.columnReturnString);

			for(int i =0; i < reps; i++) {
				d4m.next();
			}
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();
			System.out.println(d4m.rowReturnString+","+d4m.columnReturnString);
			print(d4m);
			System.out.println(">>>> SIZE = "+size+"  <<<<");
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
		} catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			d4m.close();
			d4m = null;
			System.out.println("**************** END  TEST 2 ***********************");
			System.out.println("**************** +++++++++++ ***********************");

		}

	}

	@Test	
	public void test3() {
		System.out.println("**************** START  TEST 3 ***********************");
		//		String rows="row_0000000000,";
		String rows="row_0000000000,:,row_0000000003,";
		String cols="foo_foo0,:,zoo_foo1,";
		String family= this.columnFamily;
		String authorizations="";
		//d4m.reset();
		D4mDbQuery d4m = new D4mDbQuery(instanceName, host, table, username, password);
		try {
			d4m.setLimit(5);
			d4m.doTest = true;
			d4m.doMatlabQuery(rows, cols, family, authorizations);
			//System.out.println(d4m.rowReturnString+","+d4m.columnReturnString);

			d4m.next();
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();
			//System.out.println("ROWS:"+d4m.rowReturnString+"\n COLUMNS:"+d4m.columnReturnString);
			print(d4m);
			System.out.println("3>>>> SIZE = "+size+"  <<<<");
			Assert.assertTrue(size == 10);
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
		catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			d4m.close();

			System.out.println("**************** END  TEST 3 ***********************");
			System.out.println("****************************************************");
		}
	}
	@Test	
	public void test4() {
		System.out.println("****************===============***********************");

		System.out.println("**************** START  TEST 4 ***********************");

		//		String rows="row_0000000000,";
		String rows=":";
		String cols="foo_foo0,:,foo_foo1,";
		String family= this.columnFamily;
		String authorizations="";

		D4mDbQuery d4m = new D4mDbQuery(instanceName, host, table, username, password);
		try {
			d4m.setLimit(5);
			d4m.doTest = true;
			d4m.doMatlabQuery(rows, cols, family, authorizations);
			System.out.println(d4m.rowReturnString+","+d4m.columnReturnString);

			//d4m.next();
			D4mDbResultSet results = d4m.testResultSet;
			ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
			int size = rowList.size();
			System.out.println(d4m.rowReturnString+","+d4m.columnReturnString);
			print(d4m);
			System.out.println("4 >>>> SIZE = "+size+"  <<<<");
			Assert.assertTrue(size == 3);
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
		catch(Exception e) {
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
			System.out.println("**************** END  TEST 4 ***********************");
			System.out.println("**************** +++++++++++ ***********************");

		}

	}


	//	@Test	
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
			System.out.println(d4m.rowReturnString+","+d4m.columnReturnString);

			print(d4m);

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

			d4m.close();
			System.out.println("**************** END  TEST 6 ***********************");
			System.out.println("**************** %%%%%%%%%%% ***********************");

		}

	}

	/*
	 *    row=a,:,h,
	 *    col=b,:,z,
	 *  
	 *    row=a*,:,h
	 */
	public static void print(D4mDbQuery d4m) {
		//		String rowresults = d4m.rowReturnString;
		//		String colresults = d4m.columnReturnString;
		//		log.info(rowresults+","+colresults);
		//		System.out.println(rowresults+","+colresults);

		D4mDbResultSet results = d4m.testResultSet;
		ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
		for(D4mDbRow row : rowList) {
			System.out.println(count + "  "+ row.toString());
			count++;
		}
		System.out.println(" Query elapsed time (sec) =  "+results.getQueryTime());
		System.out.println(" Total Number of rows "+rowList.size());
	}

}
