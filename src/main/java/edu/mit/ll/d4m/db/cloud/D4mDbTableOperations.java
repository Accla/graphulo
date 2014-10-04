package edu.mit.ll.d4m.db.cloud;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.logging.Logger;



//import org.apache.accumulo.core.client.IteratorSetting;
//import org.apache.accumulo.core.data.Key;
//import org.apache.accumulo.core.data.Value;
//import org.apache.accumulo.core.iterators.Combiner;
//import org.apache.accumulo.core.iterators.IteratorEnvironment;
//import org.apache.accumulo.core.iterators.IteratorUtil;
//import org.apache.accumulo.core.iterators.LongCombiner;
//import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
//import org.apache.accumulo.core.iterators.TypedValueCombiner;
//import org.apache.accumulo.core.iterators.ValueFormatException;
//import org.apache.accumulo.core.iterators.conf.ColumnSet;
//import org.apache.accumulo.core.iterators.user.MaxCombiner;
//import org.apache.accumulo.core.iterators.user.MinCombiner;
//import org.apache.accumulo.core.iterators.user.SummingCombiner;
//import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
//import org.apache.accumulo.core.util.ArgumentChecker;
//import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;



//import edu.mit.ll.cloud.connection.AccumuloConnection;
import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.util.D4mQueryUtil;
import edu.mit.ll.d4m.db.cloud.util.ArgumentChecker;



/**
 * @author wi20909
 */
public class D4mDbTableOperations extends D4mParent {
	private static  Logger log = Logger.getLogger(D4mDbTableOperations.class.getName());
	public String rowReturnString = "";
	public String columnReturnString = "";
	public String valueReturnString = "";
	private long timeout=120000l; //120s
	private ConnectionProperties connProps = new ConnectionProperties();

	D4mTableOpsIF d4mTableOp = null;
	public D4mDbTableOperations() {
		super();
	}

	public D4mDbTableOperations(ConnectionProperties connProps) {
		super();
		this.connProps = connProps;
	}
	public D4mDbTableOperations(String instanceName, String host, String username, String password) {
		super();
		this.connProps.setHost(host);
		this.connProps.setInstanceName(instanceName);
		this.connProps.setUser(username);
		this.connProps.setPass(password);

	}

	public D4mDbTableOperations(String instanceName, String host, String username, String password, String cloudType) {
		super();
		init(instanceName, host,username,password,cloudType);

	}

	public void init(String instanceName, String host, String username, String password,String cloudType) {
		this.connProps.setHost(host);
		this.connProps.setInstanceName(instanceName);
		this.connProps.setUser(username);
		this.connProps.setPass(password);
		setCloudType(cloudType);
		//		doInit();
	}
	private void doInit() {
		String instanceName = this.connProps.getInstanceName();
		String host = this.connProps.getHost();
		String username = this.connProps.getUser();
		String password = this.connProps.getPass();
		if(d4mTableOp == null)
			d4mTableOp = D4mFactory.createTableOperations(instanceName, host, username, password);

	}

	public void createTable(String tableName) {
		doInit();
		this.d4mTableOp.createTable(tableName);
	}

	public void deleteTable(String tableName) {
		doInit();
		this.d4mTableOp.deleteTable(tableName);
	}





	
	/*  
	 *  Get the total number of entries for the specified table names
	 *  tableNames   list of table names of interest	
	 */
	public long getNumberOfEntries(ArrayList<String>  tableNames)  {
		doInit();
		long retVal= this.d4mTableOp.getNumberOfEntries(tableNames);

		return retVal;
	}

	/* Bad idea: Conflicts with VersioningIterator with default priority 20
	 * Used to help specify unique priorities.  We don't really care which iterator gets what priority as system iterators 
	 * will always run before user iterators anyway, but the priorities need to be unique.  If one is taken, try the next successive integer.
	 /
	protected static int getNextPriority() {
		if (++priorityCounter > 0)
			return priorityCounter;
		else
			return priorityCounter = 1; // Wow, we actually overflowed an int...
	}
	private static int priorityCounter = 1;*/

	//	public static enum CombiningType { 
	/*****************************************************************************************/
	/*************************** ADD NEW COMBINER CLASS TYPES HERE ***************************/
	/*****************************************************************************************/
	// The second number is the statically assigned priority of the combiner (lower is higher).
	//		Only matters if more than one combiner is set on a column.
	//		Note that 20 is the VersioningIterator combiner's priority -- don't go above that!
	//		SUM(SummingCombiner.class, 7),
	//		MAX(MaxCombiner.class, 8),
	//		MIN(MinCombiner.class, 9),
	//		SUM_DECIMAL(BigDecimalSummingCombiner.class, 10), 
	//		MAX_DECIMAL(BigDecimalMaxCombiner.class, 11),
	//		MIN_DECIMAL(BigDecimalMinCombiner.class, 12);

	//		private Class<? extends Combiner> cl;
	//		private int combinerPriority;
	//
	//		static final String PREFIX = "CombiningType_";
	//		private static Map<String,CombiningType> nameMap;
	//		private static Map<Class<? extends Combiner>, CombiningType> classMap;
	//
	//		static {
	//			nameMap = new HashMap<String,CombiningType>();
	//			classMap = new HashMap<Class<? extends Combiner>, CombiningType>();
	//			for (CombiningType ct : CombiningType.values()) {
	//				nameMap.put(ct.name().toUpperCase(), ct);
	//				classMap.put(ct.cl, ct);
	//			}
	//		}
	//
	//		CombiningType(Class<? extends Combiner> cl, int combinerPriority) {
	//			this.cl = cl;
	//			this.combinerPriority = combinerPriority;
	//		}

	/**
	 * Lookup a CombiningType by name (case insensitive)
	 * @param name
	 * @return null if name not present, or the CombiningType if present
	 */
	//		public static CombiningType getByName(final String name) {
	//			return nameMap.get(name.toUpperCase());
	//		}
	//		public static CombiningType getByClass(final Class<?> name) {
	//			return classMap.get(name);
	//		}
	//		public static CombiningType getByClass(final String className) {
	//			try {
	//				return getByClass(Class.forName(className));
	//			} catch(ClassNotFoundException e) {
	//				return null;
	//			}
	//		}
	//
	//		public Class<? extends Combiner> getCl() {
	//			return cl;
	//		}
	//
	//		public int getCombinerPriority() {
	//			return combinerPriority;
	//		}
	//		public String getIteratorName() {
	//			return PREFIX+this.name();
	//		}

	/* *************************************************************************************************
	 * Begin Combiner Class Definitions
	 */
	/*
		public static class BigDecimalSummingCombiner extends TypedValueCombiner<BigDecimal>
		{
			private final static BigDecimalEncoder BDE = new BigDecimalEncoder();
			@Override
			  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
			    super.init(source, options, env);
			    setEncoder(BDE);
			  }

			@Override
			public BigDecimal typedReduce(Key key, Iterator<BigDecimal> iter) {
				if (!iter.hasNext())
					return null;
				BigDecimal sum = iter.next();
				while (iter.hasNext()) {
			      sum = sum.add(iter.next());
			    }
				return sum;
			}
		}
		public static class BigDecimalMaxCombiner extends TypedValueCombiner<BigDecimal>
		{
			private final static BigDecimalEncoder BDE = new BigDecimalEncoder();
			@Override
			  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
			    super.init(source, options, env);
			    setEncoder(BDE);
			  }

			@Override
			public BigDecimal typedReduce(Key key, Iterator<BigDecimal> iter) {
				if (!iter.hasNext())
					return null;
				BigDecimal max = iter.next();
				while (iter.hasNext()) {
			      max = max.max(iter.next());
			    }
				return max;
			}
		}
		public static class BigDecimalMinCombiner extends TypedValueCombiner<BigDecimal>
		{
			private final static BigDecimalEncoder BDE = new BigDecimalEncoder();
			@Override
			  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
			    super.init(source, options, env);
			    setEncoder(BDE);
			  }

			@Override
			public BigDecimal typedReduce(Key key, Iterator<BigDecimal> iter) {
				if (!iter.hasNext())
					return null;
				BigDecimal min = iter.next();
				while (iter.hasNext()) {
			      min = min.min(iter.next());
			    }
				return min;
			}
		}

	 */


	//	} // end CombiningType enum

	/**
	 * Provides the ability to encode scientific notation.
	 * @author dy23798
	 *
	 */
	/*
	public static class BigDecimalEncoder implements org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder<BigDecimal> {
		@Override
		public byte[] encode(BigDecimal v) {
			return v.toString().getBytes();
		}

		@Override
		public BigDecimal decode(byte[] b) throws ValueFormatException {
			try {
				return new BigDecimal(new String(b));
			} catch (NumberFormatException nfe) {
				throw new ValueFormatException(nfe);
			}
		}
	}
	 */
	/**
	 * Designates columns (which do not have to exist yet) with a Combiner. 
	 * Note: Do not add more than one combiner on a column.
	 * @param tableName
	 * @param columnStrAll In the format: "col1,col2,col3," where ',' can be any separator
	 * @param combineType "SUM", "MIN", or "MAX" or "SUM_DECIMAL", "MIN_DECIMAL", "MAX_DECIMAL"
	 * @param columnFamily An optional column family (default = "")
	 * @throws D4mException if table doesn't exist, user doesn't have permissions, or something else goes wrong
	 */
	public void designateCombiningColumns(String tableName, String columnStrAll, String combineType, String columnFamily) throws D4mException
	{
		doInit();
		this.d4mTableOp.designateCombiningColumns(tableName, columnStrAll, combineType, columnFamily);
	}
	//	public void designateCombiningColumns(String tableName, String columnStrAll, String combineType, String columnFamily) throws D4mException
	//	{
	//		ArgumentChecker.notNull(tableName, columnStrAll, combineType);
	//		if (columnFamily == null)
	//			columnFamily = "";
	//		doInit();
	//		CombiningType ct; // the type of the combiner, e.g. SUM 
	//		String[] columnStrArr = D4mQueryUtil.processParam(columnStrAll); // the columns we want to combine
	//
	//		ct = CombiningType.getByName(combineType);
	//		if (ct == null) // user did not specify "sum", "min", or "max"
	//			throw new IllegalArgumentException("user did not specify \"sum\", \"min\", or \"max\"");

	// we will get an exception if there is already a combiner on a column - let it propagate to the user
	// first, check to see if the iterator exists in this table
	//		IteratorSetting itSet = this.d4mTableOp.getIteratorSetting(tableName, ct.getIteratorName(), IteratorUtil.IteratorScope.scan); // any scope is ok
	//		if (itSet == null) {
	//			// iterator does not exist yet - create it and set it equal to the given columns
	//			itSet = new IteratorSetting(ct.getCombinerPriority(), ct.getIteratorName(), ct.cl);
	//			
	//			if (LongCombiner.class.isAssignableFrom(ct.cl)) // if using one of the Long Combiner classes, use the String en-/de-coder
	//				LongCombiner.setEncodingType(itSet, LongCombiner.Type.STRING);
	//LongCombiner.setEncodingType(itSet, BigDecimalEncoder.class);
	//			TypedValueCombiner.setLossyness(itSet, true); // silently ignore bad values
	//			
	//			List<IteratorSetting.Column> combineColumns = new LinkedList<IteratorSetting.Column>();
	//			for (String column : columnStrArr)
	//				combineColumns.add(new IteratorSetting.Column(columnFamily, column));
	//			Combiner.setColumns(itSet, combineColumns);
	//			this.d4mTableOp.addIterator(tableName, itSet); // add to majc, minc, scan
	//		}
	//		else {
	// iterator already exists - get it and add to the columns it already has
	//			String allColumnString = itSet.getOptions().get("columns"); // use ColumnSet.decodeColumns if we want the original text
	//			assert allColumnString != null && !allColumnString.isEmpty();

	// ensure the column is not already in there
	//			StringBuffer sb = new StringBuffer(allColumnString);
	//			for (String column : columnStrArr)
	//				if (!allColumnString.contains(ColumnSet.encodeColumns(new Text(columnFamily), new Text(column))))
	//					sb.append(',').append(ColumnSet.encodeColumns(new Text(columnFamily), new Text(column)));
	//			itSet.addOption("columns", sb.toString()); // overwrite previous column setting

	// remove old iterator and add new one with same priority
	//			this.d4mTableOp.removeIterator(tableName, ct.getIteratorName(), EnumSet.allOf(IteratorUtil.IteratorScope.class));
	//			this.d4mTableOp.addIterator(tableName, itSet); // add to majc, minc, scan
	//		}

	/*Map<String,IteratorSetting> cfgMap = new HashMap<String,IteratorSetting>(columnStrArr.length);
		// first, check for any conflicts - we don't want to add some but not all iterators
		for (String column : columnStrArr) {
			IteratorSetting cfg = new IteratorSetting(priorityCounter++, "CombiningType_"+column, cl); // remember only one iterator can take a priority slot
			while(true) {
				try {
					this.d4mTableOp.checkIteratorConflicts(tableName, cfg, EnumSet.allOf(IteratorUtil.IteratorScope.class));
					break;
				} catch(IllegalArgumentException e) {
					if (e.getMessage().contains("iterator priority conflict"))
						cfg.setPriority(priorityCounter++);
					else
						throw e;
				}
			}
			cfgMap.put(column, cfg);
		}

		// now that we know the columns are ok, add the appropriate iterator to each column
		for (Entry<String,IteratorSetting> entry : cfgMap.entrySet())
		{
			IteratorSetting cfg = entry.getValue();
			List<IteratorSetting.Column> combineColumns = Collections.singletonList(new IteratorSetting.Column(columnFamily, entry.getKey())); // column Family is ""
			LongCombiner.setEncodingType(cfg, LongCombiner.Type.STRING);
			Combiner.setColumns(cfg, combineColumns);
			this.d4mTableOp.addIterator(tableName, cfg); // add to majc, minc, scan
		}*/
	//	}

	/**
	 * 
	 * @param tableName
	 * @return A nice tabular view of the Combiners present with each column they are active on in the given table
	 * @throws D4mException if table doesn't exist, user doesn't have permissions, or something else goes wrong
	 */
	public String listCombiningColumns(String tableName) throws D4mException
	{
		doInit();
		String columnsList = this.d4mTableOp.listCombiningColumns(tableName); 
		return columnsList;
	}
	//	public String listCombiningColumns(String tableName) throws D4mException
	//	{
	//		ArgumentChecker.notNull(tableName);
	//		doInit();
	//		StringBuffer sb = new StringBuffer();
	//
	// for each combiningtype
	//		for (CombiningType ct : CombiningType.values())
	//		{
	//			IteratorSetting itSet = this.d4mTableOp.getIteratorSetting(tableName, ct.getIteratorName(), IteratorUtil.IteratorScope.scan); // any scope is ok
	//			if (itSet == null) {
	//				// combiner does not exist in table
	//				continue;
	//			}
	//			else {
	//				sb.append(ct.name()).append('\t');
	//				// combiner exists in table; get the columns it combines
	//				String allColumnString = itSet.getOptions().get("columns"); // use ColumnSet.decodeColumns if we want the original text
	//				assert allColumnString != null && !allColumnString.isEmpty();
	//				sb.append(allColumnString).append('\n');
	//			}
	//		}
	//		return sb.toString();
	//	}

	/**
	 * Removes whatever Combiner is present on the given columns in the given table.
	 * Note: will silently ignore specified columns that do not have combiners present.
	 * @param tableName
	 * @param columnStr In the format: "col1,col2,col3," where ',' can be any separator
	 * @throws D4mException if table doesn't exist, user doesn't have permissions, or something else goes wrong
	 */
	public void revokeCombiningColumns(String tableName, String columnStr, String columnFamily) throws D4mException
	{
		doInit();
		this.d4mTableOp.revokeCombiningColumns(tableName, columnStr, columnFamily);
	}
	//	public void revokeCombiningColumns(String tableName, String columnStr, String columnFamily) throws D4mException
	//	{
	//		ArgumentChecker.notNull(tableName, columnStr);
	//		doInit();

	// split the columns with processParam(columnStr)
	//		String[] columnStrArrToRemove = D4mQueryUtil.processParam(columnStr);
	/*// prepend column families if present
		if (!columnFamily.isEmpty()) {
			for (int i = 0; i < columnStrArrToRemove.length; i++)
				columnStrArrToRemove[i] = columnFamily+':'+columnStrArrToRemove[i];
		}*/
	//		Arrays.sort(columnStrArrToRemove);

	// METHOD: For each CombiningType:
	//	For each column that CombiningType is active on:
	//		If the column should be removed, don't add it back to sb
	//	Re-add the CombiningType with the reduced column set from sb (if there are any columns left)
	//		for (CombiningType ct : CombiningType.values())
	//		{
	//			IteratorSetting itSet = this.d4mTableOp.getIteratorSetting(tableName, ct.getIteratorName(), IteratorUtil.IteratorScope.scan); // any scope is ok
	//			if (itSet == null)
	//				continue; // combiner not present
	//			String allColumnString = itSet.getOptions().get("columns"); // use ColumnSet.decodeColumns if we want the original text
	//			StringBuffer sb = new StringBuffer(); // holds the new columns to add back
	//			boolean firstAppend = true;
	//			for (String columnPairStr : allColumnString.split(",")) {
	//				Pair<Text,Text> columnPair = ColumnSet.decodeColumns(columnPairStr);
	//				if (!columnPair.getFirst().toString().equals(columnFamily))
	//					continue; // column families don't match; leave it in
	//				if (Arrays.binarySearch(columnStrArrToRemove, columnPair.getSecond().toString()) >= 0)
	//					continue; // this is one of the columns we want to remove
	//				// we want to keep this column
	//				sb.append(columnPairStr);
	//				if (firstAppend)
	//					firstAppend = false;
	//				else
	//					sb.append(',');
	//			}
	//
	//			// sb has the columns we want to keep
	//			String sToKeep = sb.toString();
	//			this.d4mTableOp.removeIterator(tableName, ct.getIteratorName(), EnumSet.allOf(IteratorUtil.IteratorScope.class));
	//			if (!sToKeep.isEmpty()) {
	//				itSet.addOption("columns", sToKeep);
	//				this.d4mTableOp.addIterator(tableName, itSet); // add to majc, minc, scan
	//			}
	//		}
	//	}

	/**
	 * Adds the splits specified to the table (does nothing if they are already there)
	 * @param tableName
	 * @param splitsStr In the format: "row1,row2,row3," where ',' can be any separator
	 * @throws D4mException 
	 */
	public void addSplits(String tableName, String splitsStr) throws D4mException
	{
		ArgumentChecker.notNull(tableName, splitsStr);
		doInit();
		String[] splitStrArr = D4mQueryUtil.processParam(splitsStr);

		this.d4mTableOp.splitTable(tableName, splitStrArr);

	}


	/**
	 * @param tableName  name of table to find splits information
	 * @return  String [0]  name of splits
	 *          String [1]  number of splits per split name
	 *          String [2]  name of tablet servers that contain the splits
	 *          
	 */
	public String[] getAllSplitsInfo(String tableName) {
	    doInit();
		String []  results = new String[]{"","",""};

		List<String>  splitNames = this.d4mTableOp.getSplits(tableName);
		if(splitNames.isEmpty() ) return results;

		try {
			List<String>  listNumSplits = this.d4mTableOp.getSplitsNumInEachTablet(tableName);
			List<String>  listTabletOfSplits = this.d4mTableOp.getTabletLocationsForSplits(tableName, splitNames);

			results[0] = concatStringListToCommaSeparatedString(splitNames);
			results[1] = concatStringListToCommaSeparatedString(listNumSplits);
			results[2] = concatStringListToCommaSeparatedString(listTabletOfSplits);


		} catch (D4mException e) {
			e.printStackTrace();
		}


		return results;
	}
	/*
	 *   Returns only the names of the splits as comma-delimited string
	 */
	public String []  getSplits(String tableName) throws Exception {
		return getSplits(tableName, true);
	}

	/*
	 *  Return the list of split names a comma-delimited string
	 *  eg   124,234,2334,5664,
	 */	
	public String getSplitsString(String tableName) throws Exception {
		String result = "";
		List<String> splitList = this.d4mTableOp.getSplits(tableName);
		result = concatStringListToCommaSeparatedString(splitList);
		return result;
		
	}
	/**
	 * Gets the current splits or the table.  If the optional second boolean is true, returns an additional comma-delimited string that holds N+1 numbers
	 * where N is the number of splits and the (i)th number is the number of entries in tablet holding the (i-1)st split and the (i)th split.
	 * @param tableName
	 * @param getNumInEachTablet Optional 2nd boolean - default false
	 * @return An array of strings where 
	 * 		index 0 holds a string of the split names (comma-delimited)
	 *      index 1 holds the number of splits per split name
	 *      index 2 holds the name of tablet servers of each split
	 *      eg [0]  1122,1223,233,4444,
	 *         [1]   1,3,4,5,
	 *         [2]   host1,host2,host3,host4,
	 * @throws Exception
	 */
	public String[] getSplits(String tableName, boolean getNumInEachTablet) throws Exception {
		ArgumentChecker.notNull(tableName);
		doInit();

		String [] results = getAllSplitsInfo(tableName);
		return results;

	}
	public String[] SAVE_getSplits(String tableName, boolean getNumInEachTablet) throws Exception
	{
		ArgumentChecker.notNull(tableName);
		doInit();
		List<String> splitList = this.d4mTableOp.getSplits(tableName,getNumInEachTablet );

		String [] result = null;
		StringBuffer sb1 = new StringBuffer();
		StringBuffer sb2 = new StringBuffer();
		boolean isFlag=false;
		for(String split: splitList) {
			if(split.equals(":") ) { 
				isFlag= true;
				continue;
			}

			if(!split.equals(":") && !isFlag)
				sb1.append(split).append(",");

			if(isFlag) {
				//Get number of entries per tablet
				sb2.append(split).append(",");
			}
		}
		if(sb2.length() ==0) {
			result = new String[] {sb1.toString()};
		} else {
			result = new String[2];
			result[0] = sb1.toString();
			result[1] = sb2.toString();
		}
		/*
		StringBuffer sb = new StringBuffer();
		for(String split: splitList) {
			sb.append(split).append(",");
		}

		if(!getNumInEachTablet) {
			result = new String[] {sb.toString()};
		} else {

			result = new String[2];
			result[0] = sb.toString();
			List<String> list = this.d4mTableOp.getSplitsNumInEachTablet(tableName);
			sb = new StringBuffer();
			for(String s : list) {
				sb.append(s).append(",");
			}
			result[1] = sb.toString();
		}
		 */
		return result;
		//*************************************************************************************************		
		//*************************************************************************************************
		//		StringBuffer sb = new StringBuffer();
		//		for (String split : splitList)
		//			sb.append(split).append(',');
		//		
		//		if (!getNumInEachTablet) {
		//			return new String[] {sb.toString()};
		//		}
		//		else {
		//			String[] result = new String[2];
		//			result[0] = sb.toString();
		//			
		//			sb = new StringBuffer();
		//			AccumuloConnection ac = new AccumuloConnection(this.connProps);
		//			final org.apache.accumulo.core.client.Scanner scanner = ac.createScanner(org.apache.accumulo.core.Constants.METADATA_TABLE_NAME/*, org.apache.accumulo.core.Constants.NO_AUTHS*/);
		//			org.apache.accumulo.core.util.ColumnFQ.fetch(scanner, org.apache.accumulo.core.Constants.METADATA_PREV_ROW_COLUMN);
		//			final Text start = new Text(ac.getNameToIdMap().get(tableName)); // check
		//			final Text end = new Text(start);
		//			end.append(new byte[] {'<'}, 0, 1);
		//			scanner.setRange(new org.apache.accumulo.core.data.Range(start, end));
		//			
		//			List<TabletStats> tabStats = this.d4mTableOp.getTabletStatsForTables(Collections.singletonList(tableName));
		//			
		//			for (Iterator<Entry<org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value>> iterator = scanner.iterator(); iterator.hasNext();) {
		//				final Entry<org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value> next = iterator.next();
		//				if (org.apache.accumulo.core.Constants.METADATA_PREV_ROW_COLUMN.hasColumns(next.getKey())) {
		//					org.apache.accumulo.core.data.KeyExtent extent = new org.apache.accumulo.core.data.KeyExtent(next.getKey().getRow(), next.getValue());
		//					final Text pr = extent.getPrevEndRow();
		//					final Text er = extent.getEndRow();
		//final String line = String.format("%-26s (%s, %s%s", extent.toString()/*.getTableId()*/, pr == null ? "-inf" : pr.toString(), er == null ? "+inf" : er.toString(),
		//	er == null ? ") Default Tablet " : "]");
		//sb.append(line).append('\n');

		// query for the entries between pr and er
		/*Scanner scanTableData = ac.createScanner(tableName);
					scanTableData.setRange(new Range(pr, er));
					//scanTableData.fetchColumnFamily( ); // todo column family in getSplits???
					//System.err.println("debug batch size: "+scanTableData.getBatchSize());
					long count = 0;
					boolean firstResult = true;
					for (Entry<Key, Value> entry : scanTableData) {
						if (notFirstScan && firstResult)
							firstResult = false;
						else
							count++;
					}
					notFirstScan = true;
					sb.append(count).append(',');*/

		//					final ByteBuffer prb = pr == null ? null : ByteBuffer.wrap(pr.getBytes());
		//					final ByteBuffer erb = er == null ? null : ByteBuffer.wrap(er.getBytes());
		//					boolean foundIt = false;
		//					// find the TabletStats object that matches the current KeyExtent
		//					for (TabletStats tabStat : tabStats) {
		//						assert tabStat.extent.table.equals(ByteBuffer.wrap(tableName.getBytes()));
		//						if ( (erb == null ? tabStat.extent.endRow == null : tabStat.extent.endRow != null && tabStat.extent.endRow.equals(erb) )
		//						   &&(prb == null ? tabStat.extent.prevEndRow == null : tabStat.extent.prevEndRow != null && tabStat.extent.prevEndRow.equals(prb))) {
		//							// found it!
		//							sb.append(tabStat.numEntries).append(',');
		//							foundIt = true;
		//							break;
		//						}
		//					}
		//					//assert foundIt;
		//					if (!foundIt)
		//						sb.append("?,");
		//					
		//				}
		//			}
		//			
		//			result[1] = sb.toString();			
		//			
		//			return result;
		//		}

	}

	/**
	 * Get the number of splits in each tablet.
	 * N+1 numbers where N is the number of splits and the (i)th number is the number of entries in
	 *  tablet holding the (i-1)st split and the (i)th split.
	 * Return a comma-delimited list
	 *  @param tableName
	 */
	public String [] getSplitsNumInEachTablet(String tableName) throws D4mException {
		List<String> list = this.d4mTableOp.getSplitsNumInEachTablet(tableName);
		StringBuffer sb = new StringBuffer();
		for(String s : list) {
			sb.append(s).append(",");
		}
		String [] result = new String [1];
		result[0] = sb.toString();
		return result;
	}

	/**
	 * Merge tablets between (startRow, endRow] on the table. 
	 * @param tableName
	 * @param startRow single row name or the empty string/null to start at first tablet server
	 * @param endRow single row name or the empty string/null to end at last tablet server
	 */
	public void mergeSplits(String tableName, String startRow, String endRow) throws D4mException
	{
		ArgumentChecker.notNull(tableName);
		doInit();
		if (startRow != null && startRow.isEmpty())
			startRow = null;
		if (endRow != null && endRow.isEmpty())
			endRow = null;
		this.d4mTableOp.merge(tableName, startRow, endRow);
	}

	/**
	 * Ensures that newSplitsString represents the state of splits of the table by merging away any splits present in the table not in newSplitsString.
	 * Merges away all splits if newSplitsString is null or empty
	 * @param tableName
	 * @param newSplitsString
	 * @throws Exception TableNotFoundException
	 */
	public void putSplits(String tableName, String newSplitsString) throws Exception // TableNotFoundException
	{
		ArgumentChecker.notNull(tableName);
		doInit();
		if (newSplitsString == null || newSplitsString.isEmpty()) {
			mergeSplits(tableName, null, null);
			return;
		}
		String oldSplitsString = getSplitsString(tableName);

		List<String> newSplitsList = Arrays.asList(D4mQueryUtil.processParam(newSplitsString));
		NavigableSet<String> oldSplitsSet = new TreeSet<String>();

		if (!oldSplitsString.isEmpty())
			oldSplitsSet.addAll(Arrays.asList(D4mQueryUtil.processParam(oldSplitsString)));

		// algorithm: first go through old list and merge anything not in new
		// then add the new set
		for (Iterator<String> iter = oldSplitsSet.iterator(); iter.hasNext(); ) {
			String oldSplit = iter.next();
			if (!newSplitsList.contains(oldSplit)) {
				// merge away oldSplit
				String before = oldSplitsSet.lower(oldSplit);
				String after  = oldSplitsSet.higher(oldSplit); // might be null for either or both
				mergeSplits(tableName, before, after);
				iter.remove(); // remove from oldSplitsSet now that we merged the split away
			}
		}
		addSplits(tableName, newSplitsString);

	}

	/*
	 * Concatenate the string to a comma-delimited string
	 */
	private String  concatStringListToCommaSeparatedString(List<String> strList) {
		StringBuffer sb = new StringBuffer();

		for(int i = 0; i < strList.size() ; i++) {
			String s = strList.get(i);
			sb.append(s).append(",");
		}
		return sb.toString();
	}


}
/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% D4M: Dynamic Distributed Dimensional Data Model
% MIT Lincoln Laboratory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% (c) <2010> Massachusetts Institute of Technology
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 */

