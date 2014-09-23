/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.mit.ll.d4m.db.cloud.accumulo.D4mDbQueryAccumulo;
import edu.mit.ll.d4m.db.cloud.D4mDbResultSet;
import edu.mit.ll.d4m.db.cloud.D4mDbRow;


/**
 * @author cyee
 *
 */
public class D4mDbQueryTest {

    static int count=0;
	/**
	 * 
	 */
	public D4mDbQueryTest() {
		// TODO Auto-generated constructor stub
	}

	public static void print(D4mDbQueryAccumulo d4m) {


		D4mDbResultSet results = d4m.testResultSet;
		ArrayList<D4mDbRow> rowList = results.getMatlabDbRow();
		if(rowList != null) {
		for(D4mDbRow row : rowList) {
			System.out.println(count + "  "+ row.toString());
			count++;
		}
		System.out.println(" Query elapsed time (sec) =  "+results.getQueryTime());
		System.out.println(" Total Number of rows "+rowList.size());
		}
	}

}
