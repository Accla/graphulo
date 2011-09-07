package edu.mit.ll.d4m.db.cloud;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
//import java.util.logging.Logger;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.data.Key;
import cloudbase.core.data.PartialKey;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import edu.mit.ll.cloud.connection.CloudbaseConnection;
import edu.mit.ll.cloud.connection.ConnectionProperties;


/**
 * @author William Smith
 */
public class D4mDbQuery {
	private static Logger log = Logger.getLogger(D4mDbQuery.class);
	private String tableName = "";
	private int numberOfThreads = 50;
	public String rowReturnString = "";
	public String columnReturnString = "";
	public String valueReturnString = "";
	public final String newline =   "\n"; // "\n" is necessary for correct parsing. //System.getProperty("line.separator");
	public boolean doTest = false;
	private static final String KEY_RANGE = "KEY_RANGE";
	private static final String REGEX_RANGE = "REGEX_RANGE";
	private static final String POSITIVE_INFINITY_RANGE = "POSITIVE_INFINITY_RANGE";
	private static final String NEGATIVE_INFINITY_RANGE = "NEGATIVE_INFINITY_RANGE";

	private ConnectionProperties connProps = new ConnectionProperties();
	private String family = "";
	private int limit=0;
	private int count=0;
	private StringBuilder sbRowReturn = new StringBuilder();
	private StringBuilder sbColumnReturn = new StringBuilder();
	private StringBuilder sbValueReturn = new StringBuilder();
	ArrayList<D4mDbRow> rowList = new ArrayList<D4mDbRow>();  //for testing

	private boolean startRowInclusive=true;
	private boolean endRowInclusive=true;
	private boolean positiveInfinity=false;
	private boolean doAllRanges=false;
	
	Iterator<Entry<Key, Value>> scannerIter =null;
	
	private ConcurrentLinkedQueue <Entry<Key, Value>> dataQue=new ConcurrentLinkedQueue<Entry<Key,Value>>();
	public D4mDbQuery() {
		this.count=0;
		this.limit=0;
	}
	/**
	 * Constructor that may use ZooKeeperInstance or MasterInstance to connect
	 * to CB.
	 * 
	 * @param connProps
	 * @param table
	 */
	public D4mDbQuery(ConnectionProperties connProps, String table) {
		this();
		this.tableName = table;
		this.connProps = connProps;
		this.numberOfThreads = this.connProps.getMaxNumThreads();
	}

	/**
	 * Constructor that uses ZooKeeperInstance to connect to CB.
	 * 
	 * @param instanceName
	 * @param host
	 * @param table
	 * @param username
	 * @param password
	 */
	public D4mDbQuery(String instanceName, String host, String table, String username, String password) {
		this();
		this.tableName = table;
		this.connProps.setHost(host);
		this.connProps.setInstanceName(instanceName);
		this.connProps.setUser(username);
		this.connProps.setPass(password);
	}

	public D4mDbResultSet getAllData() throws CBException, TableNotFoundException, CBSecurityException {

		D4mDbResultSet results = new D4mDbResultSet();
		ArrayList<D4mDbRow> rowList = new ArrayList<D4mDbRow>();
		//CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);
		Scanner scanner = getScanner(); //cbConnection.getScanner(tableName);
		scanner.fetchColumnFamily(new Text(this.family));
		long start = System.currentTimeMillis();

		Iterator<Entry<Key, Value>> scannerIter = scanner.iterator();
		while (scannerIter.hasNext()) {
			Entry<Key, Value> entry = (Entry<Key, Value>) scannerIter.next();
			String rowKey = entry.getKey().getRow().toString();
			String value = new String(entry.getValue().get());
			String column = entry.getKey().getColumnQualifier().toString();//.replace(this.family, "");//new String(entry.getKey().getColumnQualifier().toString());

			if (this.doTest) {
				this.saveTestResults(rowKey, column.replace(this.family, ""), value);
			}
			if(buildReturnString(rowKey, column, value)) {
				//saveDataToQue(entry);
				break;
			}
		}

		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(rowList);
		return results;
	}

	/*
	 * This method build the results of row, column, and value strings
	 *   Return false if the count is less than the limit, or if the limit is infinite
	 *   Return true if the count has reach the limit.
	 * 
	 */
	private boolean buildReturnString(String rowKey,  String column, String value) {
		boolean isDone=false;

		if(limit == 0 || this.count < this.limit) {
			this.sbRowReturn.append(rowKey + newline);
			this.sbColumnReturn.append(column.replace(this.family, "") + newline);
			this.sbValueReturn.append(value + newline);
			this.count++;
		} else  {
			isDone = true;
		}
		return isDone;
	}

	/*
	 *   If there is a large amount of results and a limit is set, then 
	 *   remaining entries will be stored in a queue.
	 *   NEXT can be called to return more results.
	 */
	private void saveDataToQue(Entry<Key, Value> entry) {
		this.dataQue.add(entry);
	}
	/*
	 * Next method will return the next chunk of data.
	 * Chunk of data returned is determined by the limit.
	 * For example, if the limit is set to 100, then it would the 100 results each time NEXT is called.
	 * 
	 */
	public void next() {
		this.count=0;
		clearBuffers();
		while(!this.dataQue.isEmpty() && this.count < this.limit) {
			this.count++;
			Entry<Key, Value> entry = (Entry<Key, Value>) this.dataQue.poll();
			if(entry == null) break;
			String rowKey = entry.getKey().getRow().toString();
			String value = new String(entry.getValue().get());
			String column = entry.getKey().getColumnQualifier().toString();//.replace(this.family, "");//new String(entry.getKey().getColumnQualifier().toString());
			buildReturnString(rowKey,column,value);
		}
		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

	}
	public HashMap<String, String> assocColumnWithRow(String rows, String cols) {

		HashMap<String, Object> rowMap = this.processParam(rows);
		HashMap<String, Object> columnMap = this.processParam(cols);
		String[] rowArray = (String[]) rowMap.get("content");
		String[] columnArray = (String[]) columnMap.get("content");

		HashMap<String, String> resultMap = new HashMap<String, String>();
		for (int i = 0; i < rowArray.length; i++) {
			resultMap.put(rowArray[i], columnArray[i]);
		}
		return resultMap;
	}

	public HashMap<String, String> loadColumnMap(String cols) {

		HashMap<String, Object> columnMap = this.processParam(cols);
		String[] columnArray = (String[]) columnMap.get("content");

		HashMap<String, String> resultMap = new HashMap<String, String>();
		for (int i = 0; i < columnArray.length; i++) {
			resultMap.put(columnArray[i], columnArray[i]);
		}
		return resultMap;
	}

	public HashMap<String, String> loadRowMap(String rows) {

		HashMap<String, Object> rowMap = this.processParam(rows);
		String[] rowArray = (String[]) rowMap.get("content");

		HashMap<String, String> resultMap = new HashMap<String, String>();
		for (int i = 0; i < rowArray.length; i++) {
			resultMap.put(rowArray[i], rowArray[i]);
		}
		return resultMap;
	}

	public boolean isRangeQuery(String[] paramContent) {
		boolean rangeQuery = false;
		/*
		 * Range Queries are the following 'a,:,b,',  'a,:,end,',  ',:,b,'.
		 *  Note: Negative infinity Range a*,
		 */

		if (paramContent.length == 1) {
			if (paramContent[0].contains("*")) {
				rangeQuery = true;
			}
		}
		if (paramContent.length == 3) {
			if (paramContent[1].contains(":")) {
				rangeQuery = true;
			}
		}
		return rangeQuery;
	}

	public String getRangeQueryType(String[] paramContent) {
		/*
		 * Range Queries are the following 'a,:,b,',  'a,:,end,',  ',:,b,'. 
		 * Note: Negative Infinity Range a*,
		 */
		String rangeQueryType = "";
		if (paramContent[0].contains("*")) {
			rangeQueryType = D4mDbQuery.REGEX_RANGE;
		}
		if (paramContent.length == 3) {
			if (paramContent[1].contains(":")) {
				rangeQueryType = D4mDbQuery.KEY_RANGE;
			}
		}
		if (paramContent.length == 3) {
			if (paramContent[1].contains(":") && paramContent[2].toLowerCase().contains("end")) {
				rangeQueryType = D4mDbQuery.POSITIVE_INFINITY_RANGE;
			}
			if (paramContent[1].contains(":") && paramContent[0].equals("")) {
				rangeQueryType = D4mDbQuery.NEGATIVE_INFINITY_RANGE;
			}
		}
		return rangeQueryType;
	}

	public D4mDbResultSet doMatlabQuery(String rows, String cols, String family, String authorizations) throws CBException, CBSecurityException, TableNotFoundException {
		this.family = family;
		connProps.setAuthorizations(authorizations.split(","));
		clearBuffers();
		return doMatlabQuery(rows, cols);
	}

	private D4mDbResultSet doMatlabQuery(String rows, String cols) throws CBException, CBSecurityException, TableNotFoundException {

		if ((!rows.equals(":")) && (cols.equals(":"))) {

			HashMap<String, Object> rowMap = this.processParam(rows);
			String[] paramContent = (String[]) rowMap.get("content");
			// System.out.println("this.isRangeQuery(paramContent)="+this.isRangeQuery(paramContent));
			if (this.isRangeQuery(paramContent)) {
				return this.doMatlabRangeQueryOnRows(rows, cols);
			}
			else {
				return this.doMatlabQueryOnRows(rows, cols);
			}
		}
		if ((rows.equals(":")) && (!cols.equals(":"))) {
			return this.doMatlabQueryOnColumns(rows, cols);
		}
		if ((rows.equals(":")) && (cols.equals(":"))) {
			return this.getAllData();
		}
		if( !rows.startsWith(":") && !rows.equals(":") && (!cols.startsWith(":")) && (!cols.equals(":")) ) {
			return this.searchByRowAndColumn(rows, cols, null,null);
		}
		HashMap<String, String> rowMap = this.assocColumnWithRow(rows, cols);
		D4mDbResultSet results = new D4mDbResultSet();
		ArrayList<D4mDbRow> rowList = new ArrayList<D4mDbRow>();
		//CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);
		HashSet<Range> ranges = this.loadRanges(rowMap);
		BatchScanner scanner =  getBatchScanner(); // cbConnection.getBatchScanner(this.tableName, this.numberOfThreads);
		scanner.fetchColumnFamily(new Text(this.family));
		scanner.setRanges(ranges);

		long start = System.currentTimeMillis();

		Iterator<Entry<Key, Value>> scannerIter = scanner.iterator();
		while (scannerIter.hasNext()) {
			Entry<Key, Value> entry = (Entry<Key, Value>) scannerIter.next();
			String rowKey = entry.getKey().getRow().toString();
			String column = entry.getKey().getColumnQualifier().toString();//new String(entry.getKey().getColumnQualifier().toString());
			String value = new String(entry.getValue().get());
			String finalColumn = column.replace(this.family, "");

			if ((rowMap.containsKey(rowKey)) && (rowMap.containsValue(finalColumn))) {

				if (this.doTest) {
					this.saveTestResults(rowKey, finalColumn, value);
				}

				if(this.buildReturnString(rowKey, finalColumn, value)) {
						//saveDataToQue(entry);
						break;
					}
			}
		}

		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(rowList);
		return results;
	}

	public D4mDbResultSet doMatlabQueryOnRows(String rows, String cols, String family, String authorizations) throws CBException, CBSecurityException, TableNotFoundException {
		this.family = family;
		connProps.setAuthorizations(authorizations.split(","));
		return doMatlabQueryOnRows(rows, cols);
	}

	private D4mDbResultSet doMatlabQueryOnRows(String rows, String cols) throws CBException, CBSecurityException, TableNotFoundException {

		HashMap<String, String> rowMap = this.loadRowMap(rows);
		D4mDbResultSet results = new D4mDbResultSet();
		ArrayList<D4mDbRow> rowList = new ArrayList<D4mDbRow>();
		//CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);
		HashSet<Range> ranges = this.loadRanges(rowMap);
		BatchScanner scanner = getBatchScanner();//cbConnection.getBatchScanner(this.tableName, this.numberOfThreads);
		scanner.fetchColumnFamily(new Text(this.family));
		scanner.setRanges(ranges);
		long start = System.currentTimeMillis();

		Iterator<Entry<Key, Value>> scannerIter = scanner.iterator();
		while (scannerIter.hasNext()) {
			Entry<Key, Value> entry = (Entry<Key, Value>) scannerIter.next();
			String rowKey = entry.getKey().getRow().toString();
			String column = entry.getKey().getColumnQualifier().toString();//new String(entry.getKey().getColumnQualifier().toString());
			String finalColumn = column;//column.replace(this.family, "");

			if (rowMap.containsKey(rowKey)) {
				String value = new String(entry.getValue().get());

				if (this.doTest) {
					D4mDbRow row = new D4mDbRow();
					row.setRow(rowKey);
					row.setColumn(finalColumn);
					row.setValue(value);
					rowList.add(row);
				}

				if(this.buildReturnString(rowKey, finalColumn, value)) {
					saveDataToQue(entry);
					//break;
				}
			}
		}
		scanner.close();
		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(rowList);
		return results;
	}

	public D4mDbResultSet doMatlabRangeQueryOnRows(String rows, String cols, String family, String authorizations) throws CBException, CBSecurityException, TableNotFoundException {
		this.family = family;
		connProps.setAuthorizations(authorizations.split(","));
		return doMatlabRangeQueryOnRows(rows, cols);
	}

	private D4mDbResultSet doMatlabRangeQueryOnRows(String rows, String cols) throws CBException, CBSecurityException, TableNotFoundException {

		HashMap<String, Object> rowMap = this.processParam(rows);
		String[] rowArray = (String[]) rowMap.get("content");

		HashSet<Range> ranges = new HashSet<Range>();
		//CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);
		BatchScanner scanner = getBatchScanner();//cbConnection.getBatchScanner(this.tableName, this.numberOfThreads);

		if (this.getRangeQueryType(rowArray).equals(D4mDbQuery.KEY_RANGE)) {
			// System.out.println("queryType="+this.KEY_RANGE+
			// " rowArray[0]="+rowArray[0]+" rowArray[2]="+rowArray[2]+"<");
			Key startKey = new Key(new Text(rowArray[0]));
			Key endKey = new Key(new Text(rowArray[2]));
			//Range range = new Range(startKey, true, endKey.followingKey(1), false);
			Range range = new Range(startKey, true, endKey.followingKey(PartialKey.ROW), false);
			ranges.add(range);
			scanner.setRanges(ranges);
			// Note; there is a bug in CB 1.1 for ranges including end key,
			// use "endKey.followingKey(1), false" work around
		}

		if (this.getRangeQueryType(rowArray).equals(D4mDbQuery.POSITIVE_INFINITY_RANGE)) {
			// System.out.println("queryType="+this.POSITIVE_INFINITY_RANGE+
			// " rowArray[0]="+rowArray[0]);
			Key startKey = new Key(new Text(rowArray[0]));
			Range range = new Range(startKey, true, null, true);
			ranges.add(range);
			scanner.setRanges(ranges);
		}

		if (this.getRangeQueryType(rowArray).equals(D4mDbQuery.NEGATIVE_INFINITY_RANGE)) {
			// System.out.println("queryType="+this.NEGATIVE_INFINITY_RANGE+
			// " rowArray[0]="+rowArray[0]);
			Key endKey = new Key(new Text(rowArray[2]));
			//Range range = new Range(null, true, endKey.followingKey(1), false);
			Range range = new Range(null, true, endKey.followingKey(PartialKey.ROW), false);
			ranges.add(range);
			scanner.setRanges(ranges);
			// Note; there is a bug in CB 1.1 for ranges including end key,
			// use "endKey.followingKey(1), false" work around
		}

		if (this.getRangeQueryType(rowArray).equals(D4mDbQuery.REGEX_RANGE)) {
			// System.out.println("queryType="+this.REGEX_RANGE+
			// " rowArray[0]="+rowArray[0]);
			String regexParams = this.regexMapper(rowArray[0]);
			scanner.setRowRegex(regexParams);
			Range range = new Range();
			ranges.add(range);
			scanner.setRanges(ranges);
		}

		D4mDbResultSet results = new D4mDbResultSet();
		ArrayList<D4mDbRow> rowList = new ArrayList<D4mDbRow>();
		long start = System.currentTimeMillis();

		Iterator<Entry<Key, Value>> scannerIter = scanner.iterator();
		while (scannerIter.hasNext()) {
			Entry<Key, Value> entry = (Entry<Key, Value>) scannerIter.next();
			String rowKey = entry.getKey().getRow().toString();
			//String column = new String(entry.getKey().getColumnQualifier().toString());
			String value = new String(entry.getValue().get());
			String finalColumn =  entry.getKey().getColumnQualifier().toString().replace(this.family, "");    //column.replace(this.family, "");

			if (this.doTest) {
				this.saveTestResults(rowKey, finalColumn, value);
			}
			if(this.buildReturnString(rowKey, finalColumn, value)) {
				//saveDataToQue(entry);
				break;
			}
		}
		scanner.close();
		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(rowList);
		return results;
	}

	public D4mDbResultSet doMatlabQueryOnColumns(String rows, String cols, String family, String authorizations) throws CBException, CBSecurityException, TableNotFoundException {
		this.family = family;
		connProps.setAuthorizations(authorizations.split(","));
		return doMatlabQueryOnColumns(rows, cols);
	}

	private D4mDbResultSet doMatlabQueryOnColumns(String rows, String cols) throws CBException, CBSecurityException, TableNotFoundException {

		HashMap<?, ?> rowMap = this.loadColumnMap(cols);
		D4mDbResultSet results = new D4mDbResultSet();
		Scanner scanner = getScanner();//cbConnection.getScanner(tableName);
		scanner.fetchColumnFamily(new Text(this.family));
		long start = System.currentTimeMillis();

		Iterator<Entry<Key, Value>> scannerIter = scanner.iterator();
		while (scannerIter.hasNext()) {
			Entry<Key, Value> entry = (Entry<Key, Value>) scannerIter.next();
			String rowKey = entry.getKey().getRow().toString();
			//String column = new String(entry.getKey().getColumnQualifier().toString());
			String finalColumn =  entry.getKey().getColumnQualifier().toString().replace(this.family, "");  //column.replace(this.family, "");

			if (rowMap.containsValue(finalColumn)) {
				String value = new String(entry.getValue().get());

				if (this.doTest) {
					this.saveTestResults(rowKey, finalColumn, value);
				}
				if(this.buildReturnString(rowKey, finalColumn, value)) {
					//saveDataToQue(entry);
					break;
				}
			}

		}
		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(rowList);
		return results;
	}

	private Scanner getScanner() throws CBException, CBSecurityException, TableNotFoundException {
		CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);
		Scanner scanner = cbConnection.getScanner(tableName);
		return scanner;
	}
	/*
	 *  How can I get a chunk of data at a time?
	 *  Can I keep the iterator?
	 *  Will it time out?
	 */
	public void getNextChunk() {

	}

	/*
	 *  Search by both row and column
	 *  
	 *  row   set the range
	 *  col   set the column qualifier
	 *  
	 *  Get batchscanner
	 */
	public D4mDbResultSet searchByRowAndColumn(String rows, String cols, String family, String authorizations)  {
		//throws CBException, CBSecurityException, TableNotFoundException {
		//		System.out.println("searchByRowAndColumn - Here I am");
		clearBuffers();
		HashMap<String, Object> rowMap = this.processParam(rows);
		HashMap<String, Object> columnMap = this.processParam(cols);
		String[] rowArray = (String[]) rowMap.get("content");
		String[] columnArray = (String[]) columnMap.get("content");
		D4mDbResultSet results = new D4mDbResultSet();
		ArrayList<D4mDbRow> rowList = new ArrayList<D4mDbRow>();
		long start = System.currentTimeMillis();

		//Set up ranges
		HashSet<Range> ranges = new HashSet<Range>();
		Range range = null;
		if(rowArray.length == 3 && rowArray[1].equals(":")) {
			if(rowArray[0].compareTo(rowArray[2]) < 0)
				range = new Range(new Text(rowArray[0]), true, new Text(rowArray[2]), true);
			else 
				range = new Range(new Text(rowArray[2]), true, new Text(rowArray[0]), true);
			//System.out.println("RANGE = "+range.toString());
			if(log.isDebugEnabled())
			log.debug("RANGE = "+range.toString());
			ranges.add(range);
		} else {
			int cnt=0;
			for(String rowKey : rowArray) {
				range = new Range(new Text(rowKey));
				if(log.isDebugEnabled())
				log.debug(cnt+" :: RANGE = "+range.toString());
				cnt++;
				ranges.add(range);
			}
		}

		if(family != null || authorizations != null)
			setFamilyAndAuthorizations(family,authorizations);

		//loop over columns
		//reset column by scanner.clearColumns()s
		//set new columns fetchColumn(fam,col)
		//iterate
		//fill output buffer
		String colRegex = "";
		if(columnArray.length == 3 && columnArray[1].equals(":")) {
			String colStart1 = columnArray[0].substring(0, 1);
			String colEnd1 = columnArray[2].substring(0,1);
			colRegex = "^["+colStart1+"-"+colEnd1+"].*";
			SearchIt(ranges,colRegex);
		} else {
			for(String colKey : columnArray) {
				SearchIt(ranges,colKey);
			}
		}
		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(rowList);


		return results;
	}


	
	private void SearchIt(HashSet<Range> ranges, String col) {
		BatchScanner scanner = null;
		try {
			scanner = getBatchScanner();
			scanner.setRanges(ranges);

			//loop over columns
			//reset column by scanner.clearColumns()s
			//set new columns fetchColumn(fam,col)
			//iterate
			//fill output buffer
			String colRegex = col;
			scanner.setColumnQualifierRegex(colRegex);
			scanner.fetchColumnFamily(new Text(this.family));
			Iterator<Entry<Key, Value>> scannerIter = scanner.iterator();
			int count=0;
			while (scannerIter.hasNext()) {
				Entry<Key, Value> entry = (Entry<Key, Value>) scannerIter.next();
				String rowKey = entry.getKey().getRow().toString();
				String column = entry.getKey().getColumnQualifier().toString();
				//System.out.println(count+"BEFORE_ENTRY="+rowKey+","+column);
				String finalColumn = column.replace(this.family, "");
				boolean goodData=true;

				if(goodData) {
					String value = new String(entry.getValue().get());
					if (this.doTest) {
						this.saveTestResults(rowKey, finalColumn, value);
					}
					if(this.buildReturnString(rowKey, finalColumn, value)) {
						//saveDataToQue(entry);
						break;
					}
				}
				count++;
			}
			if(log.isDebugEnabled())
			log.debug("Num of entries found = "+count);
		} catch (CBException e) {
			e.printStackTrace();
		} catch (CBSecurityException e) {
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		}
		finally {
			scanner.close();
		}

	}

	private BatchScanner getBatchScanner() throws CBException, CBSecurityException, TableNotFoundException {
		CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);
		BatchScanner scanner = cbConnection.getBatchScanner(this.tableName, this.numberOfThreads);
		return scanner;
	}

	private void setFamilyAndAuthorizations(String family, String authorizations) {
		this.family = family;
		connProps.setAuthorizations(authorizations.split(","));

	}

	private void saveTestResults(String rowKey, String finalColumn, String value) {
		D4mDbRow row = new D4mDbRow();
		row.setRow(rowKey);
		row.setColumn(finalColumn);
		row.setValue(value);
		this.rowList.add(row);

	}
	public static void main(String[] args) throws CBException, CBSecurityException, TableNotFoundException {

		if (args.length < 5) {
			System.out.println("Usage: D4mDbQuery host table rows cols");
			return;
		}

		String hostName = args[0];
		String tableName = args[1];
		String rows = args[2];
		String cols = args[3];
		int limit = Integer.valueOf(args[4]);
		D4mDbQuery tool = new D4mDbQuery("cloudbase", hostName, tableName, "root", "ALL4114ALL");
		tool.setLimit(limit);
		tool.doTest = false;

		//System.out.println("RowReturnString=" + tool.getRowReturnString());
		//System.out.println("ColumnReturnString=" + tool.getColumnReturnString());
		//System.out.println("ValueReturnString=" + tool.getValueReturnString());
		//System.out.println("\n\n########################\n");
		//testSearchByRowAndCol();
	}


	public static void test1(D4mDbQuery tool, String rows, String cols) throws CBException, CBSecurityException, TableNotFoundException {
		D4mDbResultSet resultSet = tool.doMatlabQuery(rows, cols);
		double totalQueryTime = resultSet.getQueryTime();
		System.out.println("totalQueryTime = " + totalQueryTime);
		ArrayList<?> rowsArr = resultSet.getMatlabDbRow();

		Iterator<?> it = rowsArr.iterator();
		System.out.println("");
		System.out.println("");

		int rowsToPrint = 20 + 1;
		int counter = 0;
		while (it.hasNext()) {
			counter++;
			D4mDbRow row = (D4mDbRow) it.next();
			String rowNumber = row.getRow();
			String column = row.getColumn();
			String value = row.getValue();
			String modified = row.getModified();
			if (counter < rowsToPrint) {
				System.out.println("Row; " + rowNumber);
				System.out.println("Column; " + column);
				System.out.println("Value; " + value);
				System.out.println("Modified; " + modified);
				System.out.println("");
				System.out.println("");
			}
		}

	}
	public static void testSearchByRowAndCol() throws CBException, CBSecurityException, TableNotFoundException {
		String rowkeys="a b c ";
		String cols ="a b bb";
		String family="";
		String authorizations="";

		String instanceName="cloudbase";
		String host="f-2-10.llgrid.ll.mit.edu";
		String table="SearchRowAndColTEST";
		String username="root"; 
		String password="ALL4114ALL";

		D4mDbQuery query = new D4mDbQuery( instanceName,  host, table, username, password);
		query.searchByRowAndColumn(rowkeys, cols, family, authorizations);
		System.out.println("####RowReturnString=" + query.getRowReturnString());
		System.out.println("####ColumnReturnString=" + query.getColumnReturnString());
		System.out.println("####ValueReturnString=" + query.getValueReturnString());
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public HashMap<String, Object> processParam(String param) {
		HashMap<String, Object> map = new HashMap<String, Object>();
		String content = param.substring(0, param.length() - 1);
		String delim = param.replace(content, "");
		map.put("delimiter", delim);
		if (delim.equals("|")) {
			delim = "\\" + delim;
		}
		map.put("content", content.split(delim));
		map.put("length", content.length());
		return map;
	}

	public String getColumnReturnString() {
		return columnReturnString;
	}

	public void setColumnReturnString(String columnReturnString) {
		this.columnReturnString = columnReturnString;
	}

	public String getRowReturnString() {
		return rowReturnString;
	}

	public void setRowReturnString(String rowReturnString) {
		this.rowReturnString = rowReturnString;
	}

	public String getValueReturnString() {
		return valueReturnString;
	}

	public void setValueReturnString(String valueReturnString) {
		this.valueReturnString = valueReturnString;
	}

	public void setAuthorizations(String[] authorizations) {
		this.connProps.setAuthorizations(authorizations);
	}

	public HashSet<Range> loadRanges(HashMap<String, String> queryMap) {
		HashSet<Range> ranges = new HashSet<Range>();
		Iterator<String> it = queryMap.keySet().iterator();
		while (it.hasNext()) {
			String rowId = (String) it.next();
			Key key = new Key(new Text(rowId));
			//Range range = new Range(key, true, key.followingKey(1), false);
			Range range = new Range(key, true, key.followingKey(PartialKey.ROW), false);


			ranges.add(range);
		}
		return ranges;
	}

	private String regexMapper(String regex) {

		String charStr = regex.replace("*", "");
		String reg = "^" + charStr + "*|^" + charStr + ".";
		return reg;
	}
	public boolean isStartRowInclusive() {
		return startRowInclusive;
	}
	public void setStartRowInclusive(boolean startRowInclusive) {
		this.startRowInclusive = startRowInclusive;
	}
	public void setStartRowInclusive(Boolean startRowInclusive) {
		this.startRowInclusive = startRowInclusive;
	}

	public boolean isEndRowInclusive() {
		return endRowInclusive;
	}
	public void setEndRowInclusive(boolean endRowInclusive) {
		this.endRowInclusive = endRowInclusive;
	}
	public void setEndRowInclusive(Boolean endRowInclusive) {
		this.endRowInclusive = endRowInclusive;
	}

	public boolean isPositiveInfinity() {
		return positiveInfinity;
	}
	public void setPositiveInfinity(boolean positiveInfinity) {
		this.positiveInfinity = positiveInfinity;
	}

	public void clearBuffers() {
		this.count = 0;
		int len = this.sbRowReturn.length();
		if(len > 0)
			this.sbRowReturn = this.sbRowReturn.delete(0, len-1);
		len = this.sbColumnReturn.length();
		if(len > 0)
			this.sbColumnReturn = this.sbColumnReturn.delete(0, len-1);
		len = this.sbValueReturn.length();
		if(len > 0)
			this.sbValueReturn = this.sbValueReturn.delete(0, len);
		this.columnReturnString = "";
		this.valueReturnString = "";
		this.rowReturnString = "";
	}
	public boolean isDoAllRanges() {
		return doAllRanges;
	}
	public void setDoAllRanges(boolean doAllRanges) {
		this.doAllRanges = doAllRanges;
	}
}
/*
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % D4M: Dynamic Distributed Dimensional Data Model 
 * % MIT Lincoln Laboratory
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % (c) <2010>  Massachusetts Institute of Technology
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 */

