/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.test;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.D4mConfig;
import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations;
import edu.mit.ll.d4m.db.cloud.D4mException;
import edu.mit.ll.d4m.db.cloud.accumulo.AccumuloTableOperations;

/**
 * Test split functions for Accumulo
 *    getSplits()
 *    
 * @author CHV8091
 *
 */
public class TestAccumuloSplitInfo {
	private String instanceName = "txg-scaletest-4n32c";
	private String host = "txg-scaletest-4n32c.cloud.llgrid.ll.mit.edu:2181";
	private String username = "AccumuloUser";
	private String password = "ZcIn_EBHdVzF5J4Dvn3AIpwck";
	
	private String tableName = "kepner_GraphAnalysisTEST";

	private AccumuloTableOperations tableOps = null;
	private D4mDbTableOperations  d4mTableOps = null;
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		D4mConfig.getInstance().setCloudType(D4mConfig.ACCUMULO);

		ConnectionProperties cp;
		cp = new ConnectionProperties();
		cp.setInstanceName(instanceName);
		cp.setHost(host);
		cp.setPass(password);
		cp.setUser(username);
		tableOps = new AccumuloTableOperations(cp);
		this.d4mTableOps = new D4mDbTableOperations(instanceName, host, username, password, "Accumulo");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	/*
	 * 
	 */
	@Test
	public void test() {
		List<String> splitsList = this.tableOps.getSplits(tableName);
		System.out.println("SPLITS_LIST_SIZE="+splitsList.size());
		try {
			List<String> numSplitsList = this.tableOps.getSplitsNumInEachTablet(tableName);
			System.out.println("NUM_SPLIT_LIST_SIZE="+ numSplitsList.size());
			List<String> tabletServerNamesSplitsList =this.tableOps.getTabletLocationsForSplits(tableName, splitsList);
			System.out.println("TAB_SERVER_NAMES_LIST_SIZE="+tabletServerNamesSplitsList.size());
			for(int i = 0; i < splitsList.size() ; i++) {
				System.out.println(""+splitsList.get(i)+","+numSplitsList.get(i)+","+tabletServerNamesSplitsList.get(i));
			}
		} catch (D4mException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		}
		
	}
	
	@Test
	public void testD4mTableOperation() {
		
		try {
			String [] splitsInfo = this.d4mTableOps.getSplits(tableName);
			
		    System.out.println("SPLITS=            "+splitsInfo[0]);
		    System.out.println("NUMBER_SPLITS=     "+splitsInfo[1]);
		    System.out.println("NAME_TABLET_SERVER="+splitsInfo[2]);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
