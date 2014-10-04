/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.accumulo;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.Credentials;
//import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.trace.thrift.TInfo;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.D4mException;
import edu.mit.ll.d4m.db.cloud.D4mTableOpsIF;
import edu.mit.ll.d4m.db.cloud.util.D4mQueryUtil;
import edu.mit.ll.d4m.db.cloud.accumulo.AccumuloCombiner;
import edu.mit.ll.d4m.db.cloud.accumulo.AccumuloCombiner.CombiningType;

/**
 * @author cyee
 *
 */
public class AccumuloTableOperations implements D4mTableOpsIF {
	private static Logger log = Logger.getLogger(AccumuloTableOperations.class);

	AccumuloConnection connection= null;
	ConnectionProperties connProp= null;

	/**
	 * 
	 */
	public AccumuloTableOperations() {

	}
	public AccumuloTableOperations(ConnectionProperties connProp) {
		this.connProp = connProp;
		connect();
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#createTable(java.lang.String)
	 */
	@Override
	public void createTable(String tableName) {
		this.connection.createTable(tableName);
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#deleteTable(java.lang.String)
	 */
	@Override
	public void deleteTable(String tableName) {
		this.connection.deleteTable(tableName);
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#splitTable(java.lang.String, java.lang.String)
	 */
	@Override
	public void splitTable(String tableName, String partitions) {
		String [] pKeys = partitions.split(",");
		//Make SortedSet
		TreeSet<Text> set = new TreeSet<Text>();

		for(String pt : pKeys) {
			Text text = new Text(pt);
			set.add(text);
		}
		this.connection.addSplit(tableName, set);

	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#getNumberOfEntries(java.util.ArrayList)
	 */
	@Override
	public long getNumberOfEntries(ArrayList<String> tableNames) {
		long retval=0l;

		//Get TServers
		try {
			ArrayList<TabletServerStatus> tserverStatusList = getTabletServers();
			List<TabletStats> tabletStatsList = getTabletStatsList(tserverStatusList,  tableNames);
			retval = getNumberOfEntries(tabletStatsList);
		} catch (ThriftSecurityException e) {
			log.warn(e);
		} catch ( D4mException e)  {
			log.warn(e);    
		}
		catch (TException e) {
			log.warn(e);
		}

		// Connect to each tserver and get numEntries from each tableName
		//    Get the TabletStat

		return retval;
	}
	private long getNumberOfEntries(List<TabletStats> list) {
		long retval = 0;
		for(TabletStats ts: list) {
			log.debug("num entries = "+ts.numEntries);

			retval += ts.numEntries;
		}

		return retval;
	}

	/**
	 * Intended to be used for a single table.  Not for public use.
	 * @param tableNames
	 * @return
	 */
	@Override
	public List<TabletStats> getTabletStatsForTables(List<String> tableNames) {
		List<TabletStats> retval = null;

		//Get TServers
		try {
			ArrayList<TabletServerStatus> tserverStatusList = getTabletServers();
			retval = getTabletStatsList(tserverStatusList,  tableNames);
		} catch (ThriftSecurityException e) {
			log.warn(e);
		} catch ( D4mException e)  {
			log.warn(e);    
		}
		catch (TException e) {
			log.warn(e);
		}

		// Connect to each tserver and get numEntries from each tableName
		//    Get the TabletStat

		return retval;
	}

	private ArrayList<TabletServerStatus> getTabletServers() throws ThriftSecurityException, TException {
		ArrayList<TabletServerStatus> list = new ArrayList<TabletServerStatus>();// list of TServer info
		MasterClientService.Client client=null;
		//		MasterClientService.Iface client=null;
		try {
			MasterMonitorInfo mmi=null; 
			client = this.connection.getMasterClient();
			//changed in accumulo-1.4
			//			mmi = client.getMasterStats(null, getAuthInfo());
			TInfo tinfo = new TInfo();
			mmi = client.getMasterStats(tinfo,this.connection.getCredentials() );

			list.addAll(mmi.getTServerInfo());
		} catch(D4mException e) {
			log.warn(e);
		} finally {
			ThriftUtil.returnClient(client);
		}
		return list;
	}
	private List<TabletStats> getTabletStatsList(List<TabletServerStatus> tserverNames, List<String> tableNames) throws D4mException {
		List<TabletStats> tabStatsList=new ArrayList<TabletStats>();
		int cnt=0;
		for(TabletServerStatus tss: tserverNames) {
			cnt++;
			String tserverName = tss.name;
			log.debug("["+cnt+"] - Tserver name = "+tserverName);

			List<TabletStats> tlist = getTabletStatsList(tserverName, tableNames);

			tabStatsList.addAll(tlist);
		}
		return tabStatsList;
	}
	/*
	 * Get numEntries from tserver
	 */
	private List<TabletStats> getTabletStatsList(String tserverName, List<String> tableNames) throws D4mException {
		MasterClientService.Iface masterClient= null;
		TabletClientService.Iface tabClient = null;
		//AuthInfo authInfo  = getAuthInfo();
		List<TabletStats> tabStatsList = new ArrayList<TabletStats>();
		try {
			masterClient = this.connection.getMasterClient();
			tabClient = this.connection.getTabletClient(tserverName);
			Map<String, String> nameToIdMap = this.connection.getNameToIdMap();

			for(String tableName : tableNames) {

				String tableId = nameToIdMap.get(tableName);
				log.debug(tserverName+"-Tablet INFO ("+tableName+","+tableId+")");
				TInfo tinfo = new TInfo();

				tabStatsList.addAll(tabClient.getTabletStats(tinfo, this.connection.getCredentials() , tableId));
				//		tabStatsList.addAll(tabClient.getTabletStats(null, authInfo, tableId));
			}

		} catch (TTransportException e) {
			log.warn(e);
		} catch (ThriftSecurityException e) {
			log.warn(e);
		} catch (TException e) {
			log.warn(e);
		} finally {
			ThriftUtil.returnClient((MasterClientService.Client)masterClient);
			ThriftUtil.returnClient((TabletClientService.Client)tabClient);
		}


		return tabStatsList;
	}
	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#setConnProps(edu.mit.ll.cloud.connection.ConnectionProperties)
	 */
	@Override
	public void setConnProps(ConnectionProperties connProp) {
		this.connProp = connProp;

	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#setConnProps(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void setConnProps(String instanceName, String host, String username,
			String password) {
		this.connProp = new ConnectionProperties();
		this.connProp.setHost(host);
		this.connProp.setInstanceName(instanceName);
		this.connProp.setUser(username);
		this.connProp.setPass(password);
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#connect()
	 */
	@Override
	public void connect() {
		this.connection = new AccumuloConnection(connProp);
	}
//	public AuthInfo getAuthInfo() {
//		String user = this.connProp.getUser();
//		byte [] pw = this.connProp.getPass().getBytes();
//		String instanceId = this.connection.getInstance().getInstanceID();
//		//Accumulo-1.4 use ByteBuffer for the password in AuthInfo constructor
//		ByteBuffer pwbuffer = ByteBuffer.wrap(pw);
//		AuthInfo authinfo=new AuthInfo(user, pwbuffer, instanceId);
//		return authinfo;
//	}

	public TCredentials tCred=null;

	@Override
	public void splitTable(String tableName, String[] partitions) {
		TreeSet<Text> tset = new TreeSet<Text>();
		for(String pt : partitions) {
			tset.add(new Text(pt));
		}

		splitTable(tableName,tset);
	}

	@Override
	public void splitTable(String tableName, SortedSet<Text> partitions) {
		// TODO Auto-generated method stub
		this.connection.addSplit(tableName, partitions);

	}

	@Override
	public List<String> getSplits(String tableName) {
		Collection<Text> splitsColl=null;
		List<String> list = new ArrayList<String>();
		try {
			splitsColl = this.connection.getSplits(tableName);
			for(Text t: splitsColl) {
				String s = t.toString();
				list.add(s);
			}
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}

		return list;
	}
	/*
	 * List of splits and the number of entries in each tablet.
	 * The the splits and the numbers are demarcated by ":" in the List object
	 * For example, list looks like
	 *     split1
	 *     split2
	 *     split3
	 *     split4
	 *     :
	 *     100
	 *     200
	 *     300
	 *     400
	 *     
	 * (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#getSplits(java.lang.String, boolean)
	 */
	public List<String> getSplits(String tableName, boolean getNumInEachTablet) throws D4mException {
		ArgumentChecker.notNull(tableName);
		//doInit();
		List<String> splitList = getSplits(tableName);

		if (!getNumInEachTablet) {
			return splitList;
		}
		else {
			splitList.add(":");
            splitList.addAll(getSplitsNumInEachTablet(tableName));
			return splitList;
		}
	}

	/*
	 * This will return a list containing number of splits per split.
	 * 
	 * @param tableName   name of table related to list of splits
	 * @param splitList  list of split names
	 * @return
	 */


	@Override
	public void addIterator(String tableName, IteratorSetting cfg) throws D4mException {
		this.connection.addIterator(tableName, cfg);
	}
	@Override
	public Map<String, EnumSet<IteratorScope>> listIterators(String tableName) throws D4mException {
		return this.connection.listIterators(tableName);
	}
	@Override
	public IteratorSetting getIteratorSetting(String tableName,
			String iterName, IteratorScope scope) throws D4mException {
		return this.connection.getIteratorSetting(tableName, iterName, scope);
	}
	@Override
	public void removeIterator(String tableName, String name,
			EnumSet<IteratorScope> scopes) throws D4mException {
		this.connection.removeIterator(tableName, name, scopes);
	}
	@Override
	public void checkIteratorConflicts(String tableName, IteratorSetting cfg,
			EnumSet<IteratorScope> scopes) throws D4mException {
		this.connection.checkIteratorConflicts(tableName, cfg, scopes);

	}
	/*@Override
	public void addSplits(String tableName, SortedSet<Text> splitsSet) throws D4mException {
		this.connection.addSplit(tableName, splitsSet);

	}*/
	@Override
	public void merge(String tableName, String startRow, String endRow) throws D4mException {
		this.connection.merge(tableName, startRow, endRow);
	}

	public void designateCombiningColumns(String tableName, String columnStrAll, String combineType, String columnFamily) throws D4mException
	{
		ArgumentChecker.notNull(tableName, columnStrAll, combineType);
		if (columnFamily == null)
			columnFamily = "";
		//doInit();
		CombiningType ct; // the type of the combiner, e.g. SUM 
		String[] columnStrArr = D4mQueryUtil.processParam(columnStrAll); // the columns we want to combine

		ct = CombiningType.getByName(combineType);
		if (ct == null) // user did not specify "sum", "min", or "max"
			throw new IllegalArgumentException("user did not specify \"sum\", \"min\", or \"max\"");

		// we will get an exception if there is already a combiner on a column - let it propagate to the user
		// first, check to see if the iterator exists in this table
		//		IteratorSetting itSet = this.d4mTableOp.getIteratorSetting(tableName, ct.getIteratorName(), IteratorUtil.IteratorScope.scan); // any scope is ok
		IteratorSetting itSet = getIteratorSetting(tableName, ct.getIteratorName(), IteratorUtil.IteratorScope.scan); // any scope is ok
		if (itSet == null) {
			// iterator does not exist yet - create it and set it equal to the given columns
			itSet = new IteratorSetting(ct.getCombinerPriority(), ct.getIteratorName(), ct.cl);

			if (LongCombiner.class.isAssignableFrom(ct.cl)) // if using one of the Long Combiner classes, use the String en-/de-coder
				LongCombiner.setEncodingType(itSet, LongCombiner.Type.STRING);
			//LongCombiner.setEncodingType(itSet, BigDecimalEncoder.class);
			TypedValueCombiner.setLossyness(itSet, true); // silently ignore bad values

			List<IteratorSetting.Column> combineColumns = new LinkedList<IteratorSetting.Column>();
			for (String column : columnStrArr)
				combineColumns.add(new IteratorSetting.Column(columnFamily, column));
			Combiner.setColumns(itSet, combineColumns);
			//this.d4mTableOp.addIterator(tableName, itSet); // add to majc, minc, scan
			addIterator(tableName, itSet); // add to majc, minc, scan
		}
		else {
			// iterator already exists - get it and add to the columns it already has
			String allColumnString = itSet.getOptions().get("columns"); // use ColumnSet.decodeColumns if we want the original text
			assert allColumnString != null && !allColumnString.isEmpty();

			// ensure the column is not already in there
			StringBuffer sb = new StringBuffer(allColumnString);
			for (String column : columnStrArr)
				if (!allColumnString.contains(ColumnSet.encodeColumns(new Text(columnFamily), new Text(column))))
					sb.append(',').append(ColumnSet.encodeColumns(new Text(columnFamily), new Text(column)));
			itSet.addOption("columns", sb.toString()); // overwrite previous column setting

			// remove old iterator and add new one with same priority
			//this.d4mTableOp.removeIterator(tableName, ct.getIteratorName(), EnumSet.allOf(IteratorUtil.IteratorScope.class));
			//this.d4mTableOp.addIterator(tableName, itSet); // add to majc, minc, scan
			removeIterator(tableName, ct.getIteratorName(), EnumSet.allOf(IteratorUtil.IteratorScope.class));
			addIterator(tableName, itSet); // add to majc, minc, scan

		}

	}

	public String listCombiningColumns(String tableName) throws D4mException
	{
		ArgumentChecker.notNull(tableName);
		//	doInit();
		StringBuffer sb = new StringBuffer();

		// for each combiningtype
		for (CombiningType ct : CombiningType.values())
		{
			//			IteratorSetting itSet = this.d4mTableOp.getIteratorSetting(tableName, ct.getIteratorName(), IteratorUtil.IteratorScope.scan); // any scope is ok
			IteratorSetting itSet = getIteratorSetting(tableName, ct.getIteratorName(), IteratorUtil.IteratorScope.scan); // any scope is ok
			if (itSet == null) {
				// combiner does not exist in table
				continue;
			}
			else {
				sb.append(ct.name()).append('\t');
				// combiner exists in table; get the columns it combines
				String allColumnString = itSet.getOptions().get("columns"); // use ColumnSet.decodeColumns if we want the original text
				assert allColumnString != null && !allColumnString.isEmpty();
				sb.append(allColumnString).append('\n');
			}
		}
		return sb.toString();
	}

	public void revokeCombiningColumns(String tableName, String columnStr, String columnFamily) throws D4mException
	{
		ArgumentChecker.notNull(tableName, columnStr);
		///doInit();

		// split the columns with processParam(columnStr)
		String[] columnStrArrToRemove = D4mQueryUtil.processParam(columnStr);
		/*// prepend column families if present
		if (!columnFamily.isEmpty()) {
			for (int i = 0; i < columnStrArrToRemove.length; i++)
				columnStrArrToRemove[i] = columnFamily+':'+columnStrArrToRemove[i];
		}*/
		Arrays.sort(columnStrArrToRemove);

		// METHOD: For each CombiningType:
		//	For each column that CombiningType is active on:
		//		If the column should be removed, don't add it back to sb
		//	Re-add the CombiningType with the reduced column set from sb (if there are any columns left)
		for (CombiningType ct : CombiningType.values())
		{
			//			IteratorSetting itSet = this.d4mTableOp.getIteratorSetting(tableName, ct.getIteratorName(), IteratorUtil.IteratorScope.scan); // any scope is ok
			IteratorSetting itSet = getIteratorSetting(tableName, ct.getIteratorName(), IteratorUtil.IteratorScope.scan); // any scope is ok
			if (itSet == null)
				continue; // combiner not present
			String allColumnString = itSet.getOptions().get("columns"); // use ColumnSet.decodeColumns if we want the original text
			StringBuffer sb = new StringBuffer(); // holds the new columns to add back
			boolean firstAppend = true;
			for (String columnPairStr : allColumnString.split(",")) {
				Pair<Text,Text> columnPair = ColumnSet.decodeColumns(columnPairStr);
				if (!columnPair.getFirst().toString().equals(columnFamily))
					continue; // column families don't match; leave it in
				if (Arrays.binarySearch(columnStrArrToRemove, columnPair.getSecond().toString()) >= 0)
					continue; // this is one of the columns we want to remove
				// we want to keep this column
				sb.append(columnPairStr);
				if (firstAppend)
					firstAppend = false;
				else
					sb.append(',');
			}

			// sb has the columns we want to keep
			String sToKeep = sb.toString();

			//this.d4mTableOp.removeIterator(tableName, ct.getIteratorName(), EnumSet.allOf(IteratorUtil.IteratorScope.class));
			removeIterator(tableName, ct.getIteratorName(), EnumSet.allOf(IteratorUtil.IteratorScope.class));
			if (!sToKeep.isEmpty()) {
				itSet.addOption("columns", sToKeep);
				//				this.d4mTableOp.addIterator(tableName, itSet); // add to majc, minc, scan
				addIterator(tableName, itSet); // add to majc, minc, scan
			}
		}
	}
	/*
	 *   newSplitsString = comma-delimited list
	 * (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mTableOpsIF#putSplits(java.lang.String, java.lang.String)
	 */
	@Override
	public void putSplits(String tableName, String newSplitsString)
			throws D4mException {
		ArgumentChecker.notNull(tableName);
		if (newSplitsString == null || newSplitsString.isEmpty()) {
			merge(tableName, null, null);
			return;
		}
		List<String> oldSplitsString = getSplits(tableName);

		List<String> newSplitsList = Arrays.asList(D4mQueryUtil.processParam(newSplitsString));
		if(oldSplitsString != null ) {
			NavigableSet<String> oldSplitsSet = new TreeSet<String>(oldSplitsString);

			// algorithm: first go through old list and merge anything not in new
			// then add the new set
			for (Iterator<String> iter = oldSplitsSet.iterator(); iter.hasNext(); ) {
				String oldSplit = iter.next();
				if (!newSplitsList.contains(oldSplit)) {
					// merge away oldSplit
					String before = oldSplitsSet.lower(oldSplit);
					String after  = oldSplitsSet.higher(oldSplit); // might be null for either or both
					merge(tableName, before, after);
					iter.remove(); // remove from oldSplitsSet now that we merged the split away
				}
			}

		}
		addSplits(tableName, newSplitsString);
	}
	private void addSplits(String tableName, String splitsStr) throws D4mException
	{
		ArgumentChecker.notNull(tableName, splitsStr);

		String[] splitStrArr = D4mQueryUtil.processParam(splitsStr);

		splitTable(tableName, splitStrArr);

	}

    public final String METADATA_TABLE_NAME = "!METADATA";
    public final ColumnFQ METADATA_PREV_ROW_COLUMN = new ColumnFQ(new Text("~tab"), new Text("~pr"));

	@Override
	public List<String> getSplitsNumInEachTablet(String tableName)
			throws D4mException {
		List<String> list = new ArrayList<String>();
		AccumuloConnection ac = new AccumuloConnection(this.connProp);
		org.apache.accumulo.core.client.Scanner scanner;
		try {
			scanner = ac.createScanner(METADATA_TABLE_NAME/*, org.apache.accumulo.core.Constants.NO_AUTHS*/);
		} catch (TableNotFoundException e) {
			throw new D4mException("Table not found - "+METADATA_TABLE_NAME,e);
		}
        METADATA_PREV_ROW_COLUMN.fetch(scanner);
		final Text start = new Text(ac.getNameToIdMap().get(tableName)); // check
		final Text end = new Text(start);
		end.append(new byte[] {'<'}, 0, 1);
		scanner.setRange(new org.apache.accumulo.core.data.Range(start, end));

		List<TabletStats> tabStats = getTabletStatsForTables(Collections.singletonList(tableName));

		for (Iterator<Entry<org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value>> iterator = scanner.iterator(); iterator.hasNext();) {
			final Entry<org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value> next = iterator.next();
			if (METADATA_PREV_ROW_COLUMN.hasColumns(next.getKey())) { // may not be necessary
				org.apache.accumulo.core.data.KeyExtent extent = new org.apache.accumulo.core.data.KeyExtent(next.getKey().getRow(), next.getValue());
				final Text pr = extent.getPrevEndRow();
				final Text er = extent.getEndRow();

				final ByteBuffer prb = pr == null ? null : ByteBuffer.wrap(pr.getBytes());
				final ByteBuffer erb = er == null ? null : ByteBuffer.wrap(er.getBytes());
				boolean foundIt = false;
				// find the TabletStats object that matches the current KeyExtent
				for (TabletStats tabStat : tabStats) {
					assert tabStat.extent.table.equals(ByteBuffer.wrap(tableName.getBytes()));
					if ( (erb == null ? tabStat.extent.endRow == null : tabStat.extent.endRow != null && tabStat.extent.endRow.equals(erb) )
							&&(prb == null ? tabStat.extent.prevEndRow == null : tabStat.extent.prevEndRow != null && tabStat.extent.prevEndRow.equals(prb))) {
						// found it!
						list.add(Long.toString(tabStat.numEntries));
						//					sb.append(tabStat.numEntries).append(',');
						foundIt = true;
						break;
					}
				}
				//assert foundIt;
				if (!foundIt) {
					list.add("?");
				}
			}
		}

		return list;
	}
	@Override
	public List<String> getTabletLocationsForSplits(String tableName,
			List<String> splits) throws D4mException {
		List<String>  results = new ArrayList<String>();

		try {

			for(String splitName : splits) {
				String tablet_location = this.connection.locateTablet(tableName, splitName);
				results.add(tablet_location);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} 

		return results;
	}

	/*
	 * Concatenate the string to a comma-delimited string
	 */
	private String  concatString(List<String> strList) {
		StringBuffer sb = new StringBuffer();
		
		for(int i = 0; i < strList.size() ; i++) {
			String s = strList.get(i);
			sb.append(s).append(",");
		}
		return sb.toString();
	}

}
