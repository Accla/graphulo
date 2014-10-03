/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.hadoop.io.Text;

import edu.mit.ll.cloud.connection.ConnectionProperties;

/**
 * Table Operations interface
 * @author cyee
 *
 */
public interface D4mTableOpsIF {

	/** Create table
	 * @param tableName
	 */
	public void createTable(String tableName);
	/**
	 * Delete table
	 * @param tableName
	 */
	public void deleteTable(String tableName);
	
	/**
	 * Split table at partitions
	 * 
	 * @param tableName    name of table to split
	 * @param partitions   comma-separated list of partition names
	 */
	public void splitTable(String tableName, String partitions);
	public void splitTable(String tableName, String [] partitions);
	public void splitTable(String tableName, SortedSet<Text> partitions);


	/**
	 * Get the total number of entries for the list of tables
	 * 
	 * @param tableNames  list of tables
	 * @return
	 */
	public long getNumberOfEntries(ArrayList<String>  tableNames);
	/**
	 * Set the connection properties such as username , authorizations, etc
	 * @param connProp
	 */
	public void setConnProps(ConnectionProperties connProp);
	
	public void setConnProps(String instanceName, String host, String username, String password);
	/**
	 *  Make connection to cloud
	 */
	public void connect();
	
	public List<String> getSplits(String tableName);
	public List<String> getSplits(String tableName, boolean getNumInEachTablet) throws D4mException;
	public List<String> getSplitsNumInEachTablet(String tableName) throws D4mException;
	/*
	 *  GetTabletLocationsForSplits  will get the list of tablet server names that correspond
	 *  to the list of split names.
	 *  INPUT   tableName  table name of splits
	 *          splits     the list of splits
	 *  OUTPUT    list of tablet servers
	 */
	public List<String> getTabletLocationsForSplits(String tableName,List<String> splits) throws D4mException;

	/**
	 * Ensures that newSplitsString represents the state of splits of the table by merging away any splits present in the table not in newSplitsString.
	 * Merges away all splits if newSplitsString is null or empty
	 * @param tableName
	 * @param newSplitsString
	 * @throws Exception TableNotFoundException
	 */
	public void putSplits(String tableName, String newSplitsString) throws D4mException;
	public void addIterator(String tableName, IteratorSetting cfg) throws D4mException;
	public Map<String, EnumSet<IteratorScope>> listIterators(String tableName) throws D4mException;
	public IteratorSetting getIteratorSetting(String tableName, String iterName, IteratorScope scan) throws D4mException;
	public void removeIterator(String tableName, String name, EnumSet<IteratorScope> allOf) throws D4mException;
	public void checkIteratorConflicts(String tableName, IteratorSetting cfg, EnumSet<IteratorScope> allOf) throws D4mException;
	//public void addSplits(String tableName, SortedSet<Text> splitsSet) throws D4mException;
	public void merge(String tableName, String startRow, String endRow) throws D4mException;
	public List<TabletStats> getTabletStatsForTables(List<String> tableNames);
	
	//10-23-2012 YEE - add for Combiner functionality
	public void designateCombiningColumns(String tableName, String columnStrAll, String combineType, String columnFamily) throws D4mException;
	public String listCombiningColumns(String tableName) throws D4mException;
	public void revokeCombiningColumns(String tableName, String columnStr, String columnFamily) throws D4mException;

}
