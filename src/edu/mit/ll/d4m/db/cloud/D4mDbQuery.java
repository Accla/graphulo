package edu.mit.ll.d4m.db.cloud;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;
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
import cloudbase.core.iterators.RegExIterator;
import cloudbase.core.iterators.filter.RegExFilter;
import edu.mit.ll.cloud.connection.CloudbaseConnection;
import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.util.RegExpUtil;


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
	private int limit=0; // number of elements (column)
	private int numRows=0; //number of rows
	private int count=0;
	private long cumCount=0; //cumulative count of results retrieved
	private String rowsQuery=null;
	private String colsQuery=null;
	private HashMap<String, Object> rowMap=null;
	private HashMap<String, String> rowStringMap=null;
	private HashMap<String, Object> colMap=null;
	private String [] rowsArray = null;
	private ArrayList<Key> rowKeys=null;
	private StringBuilder sbRowReturn = new StringBuilder();
	private StringBuilder sbColumnReturn = new StringBuilder();
	private StringBuilder sbValueReturn = new StringBuilder();
	ArrayList<D4mDbRow> rowList = new ArrayList<D4mDbRow>();  //for testing

	private boolean startRowInclusive=true;
	private boolean endRowInclusive=true;
	private boolean positiveInfinity=false;
	private boolean doAllRanges=false;

	private Iterator<Entry<Key, Value>> scannerIter =null;

	private String lastRowKey = null;
	private Key startKey      = null;

	private Range startRange  = null;
	private String methodName = null;
	private Scanner scanner = null;
	private BatchScanner bscanner = null;
	public D4mDbResultSet testResultSet=null;
	public boolean hasNext=false;
	private boolean getAllData = false;
	private boolean getNext = true;
	private int index = 0;
	private LinkedList<Range> rangesList= new LinkedList<Range>();
	//private ConcurrentLinkedQueue <Entry<Key, Value>> dataQue=new ConcurrentLinkedQueue<Entry<Key,Value>>();
	public D4mDbQuery() {
		this.count=0;
		this.limit=0;
		this.cumCount = 0;
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
		this.getAllData = true;
		this.methodName="getAllData";
		D4mDbResultSet results = new D4mDbResultSet();
		//ArrayList<D4mDbRow> rowList = new ArrayList<D4mDbRow>();
		//CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);
		Scanner scanner = getScanner(); //cbConnection.getScanner(tableName);
		if(this.startRange == null) {
			//			this.startKey = new Key();
			this.startRange = new Range(this.startKey,null);
		}
		else if (this.startKey != null) {
			//Text row = this.startKey.getRow();
			this.startRange = new Range(this.startKey,null);
		}
		scanner.setRange(startRange);
		scanner.fetchColumnFamily(new Text(this.family));
		long start = System.currentTimeMillis();

		Iterator<Entry<Key, Value>> scannerIter = scanner.iterator();
		iterateOverEntries(scannerIter);

		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (double)(System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(this.rowList);
		this.testResultSet = results;
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
			log.debug("  +++ ROW="+rowKey+",COL="+column+",VAL="+value+" +++");
			this.sbRowReturn.append(rowKey + newline);
			this.sbColumnReturn.append(column.replace(this.family, "") + newline);
			this.sbValueReturn.append(value + newline);
			this.count++;
			this.cumCount++;
			if(this.limit > 0 && this.count == this.limit) {
				isDone=true;
			}
		} else  {
			this.lastRowKey = rowKey;
			isDone = true;
			//this.cumCount = this.cumCount + (long)this.count;
		}

		return isDone;
	}
	private boolean buildReturnString(Key theKey, String rowKey,  String column, String value) {
		boolean isDone = buildReturnString(rowKey, column, value);
		if(isDone) {
			//Key startkey = this.startRange.getStartKey();
			//startkey.set(theKey);
			this.startKey = theKey;
		}

		if (this.doTest) {
			this.saveTestResults(rowKey, theKey.getColumnFamily().toString() ,column, value);
		}

		return isDone;
	}
	private boolean buildReturnString(Key theKey, Value val) {
		String rowKey = theKey.getRow().toString();
		String column = theKey.getColumnQualifier().toString();
		String value = new String(val.get());
		boolean isDone = buildReturnString(theKey,rowKey, column, value);
		return isDone;
	}

	public boolean hasNext() {
		return this.hasNext;
	}
	/*
	 * Next method will return the next chunk of data.
	 * Chunk of data returned is determined by the limit.
	 * For example, if the limit is set to 100, then it would return 100 results each time NEXT is called.
	 * 
	 */
	public void next() {
		this.count=0;
		clearBuffers();
		try {
			if(this.hasNext) {
				if(this.getAllData) {
					getAllData();
				}
				else {
					doMatlabQuery(this.rowsQuery, this.colsQuery);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

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

	/*
	 *  A common method called by loadRowMap and loadColumnMap
	 */
	private HashMap<String, String>  loadMap(String queryString) {
		HashMap<String, Object> tmpObjMap = this.processParam(queryString);
		String[] contentArray = (String[]) tmpObjMap.get("content");

		//		HashMap<String, String> resultMap = new HashMap<String, String>();
		//		for (int i = 0; i < contentArray.length; i++) {
		//			resultMap.put(contentArray[i], contentArray[i]);
		//		}
		HashMap<String, String> resultMap = loadMap(contentArray);
		return resultMap;

	}

	private HashMap<String,String> loadMap (String [] contentArray) {
		HashMap<String, String> resultMap = new HashMap<String, String>();
		for (int i = 0; i < contentArray.length; i++) {
			resultMap.put(contentArray[i], contentArray[i]);
		}
		return resultMap;
	}
	public HashMap<String, String> loadColumnMap(String cols) {

		//		HashMap<String, Object> columnMap = this.processParam(cols);
		//		String[] columnArray = (String[]) columnMap.get("content");
		//			HashMap<String, String> resultMap = new HashMap<String, String>();
		//		for (int i = 0; i < columnArray.length; i++) {
		//			resultMap.put(columnArray[i], columnArray[i]);
		//		}
		HashMap<String, String> resultMap = loadMap(cols);

		return resultMap;
	}

	public HashMap<String, String> loadColumnMap(String [] cols) {
		HashMap<String, String> resultMap = loadMap(cols);
		return resultMap;
	}

	public HashMap<String, String> loadColumnMap(HashMap<String,Object> colMap) {
		String[] contentArray = (String[]) colMap.get("content");
		HashMap<String,String> resultMap = loadMap(contentArray);
		return resultMap;
	}

	public HashMap<String, String> loadRowMap(String rows) {

		//		HashMap<String, Object> rowMap = this.processParam(rows);
		//		String[] rowArray = (String[]) rowMap.get("content");
		//
		//		HashMap<String, String> resultMap = new HashMap<String, String>();
		//		for (int i = 0; i < rowArray.length; i++) {
		//			resultMap.put(rowArray[i], rowArray[i]);
		//		}
		HashMap<String, String> resultMap = loadMap(rows);
		return resultMap;
	}

	public HashMap<String, String> loadRowMap(HashMap<String, Object> rowMap) {

		String[] rowArray = (String[]) rowMap.get("content");
		//		HashMap<String, String> resultMap = new HashMap<String, String>();
		//		for (int i = 0; i < rowArray.length; i++) {
		//			resultMap.put(rowArray[i], rowArray[i]);
		//		}
		HashMap<String, String> resultMap = loadMap(rowArray);
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
		reset();
		this.rowsQuery = rows;
		this.colsQuery = cols;
		return doMatlabQuery(rows, cols);
	}

	private D4mDbResultSet doMatlabQuery(String rows, String cols) throws CBException, CBSecurityException, TableNotFoundException {

		if ((!rows.equals(":")) && (cols.equals(":"))) {

			HashMap<String, Object> rowMap = this.processParam(rows);
			this.rowMap = rowMap;
			String[] paramContent = (String[]) rowMap.get("content");
			// System.out.println("this.isRangeQuery(paramContent)="+this.isRangeQuery(paramContent));
			if (this.isRangeQuery(paramContent)) {
				log.debug("MATLAB_RANGE_QUERY_ON_ROWS");
				return this.doMatlabRangeQueryOnRows(rows, cols);
			}
			else {
				log.debug("MATLAB_QUERY_ON_ROWS");
				return this.doMatlabQueryOnRows(rows, cols);
			}
		}
		if ((rows.equals(":")) && (!cols.equals(":"))) {
			log.debug("MATLAB_QUERY_ON_COLS");
			return this.doMatlabQueryOnColumns(rows, cols);
		}
		if ((rows.equals(":")) && (cols.equals(":"))) {
			log.debug("GET_ALL_DATA");
			return this.getAllData();
		}
		if( !rows.startsWith(":") && !rows.equals(":") && (!cols.startsWith(":")) && (!cols.equals(":")) ) {
			log.debug("SEARCH_BY_ROW_&_COL");
			return this.searchByRowAndColumn(rows, cols, null,null);
		}

		//FIXME - this.rowMap<String,Object> is incompatible with rowMap<String,String>
		//String [] rowsArray = null;
		log.debug("==== AssocColumnWithRow ====");
		if(this.rowStringMap == null) {
			this.rowStringMap = this.assocColumnWithRow(rows, cols);
			this.rowMap = processParam(rows);
			String [] rowsArray = (String[])this.rowMap.get("content");
			this.rowKeys = param2keys(rowsArray);
		}
		HashMap<String, String> rowMap = this.rowStringMap;

		D4mDbResultSet results = new D4mDbResultSet();
		HashSet<Range> ranges = this.loadRanges(this.rowKeys);
		BatchScanner scanner =  getBatchScanner(); // cbConnection.getBatchScanner(this.tableName, this.numberOfThreads);
		scanner.fetchColumnFamily(new Text(this.family));
		scanner.setRanges(ranges);

		long start = System.currentTimeMillis();

		Entry<Key, Value> entry = null;
		String rowKey = null;
		Iterator<Entry<Key, Value>> scannerIter = scanner.iterator();
		while ((this.count < this.limit)||(this.hasNext=scannerIter.hasNext())) {
			entry = (Entry<Key, Value>) scannerIter.next();
			rowKey = entry.getKey().getRow().toString();
			String column = entry.getKey().getColumnQualifier().toString();//new String(entry.getKey().getColumnQualifier().toString());
			String value = new String(entry.getValue().get());
			String finalColumn = column.replace(this.family, "");

			if ((rowMap.containsKey(rowKey)) && (rowMap.containsValue(finalColumn))) {

				if(this.buildReturnString(entry.getKey(),rowKey, finalColumn, value)) {
					break;
				}
			}
		}

		//Set the Key to start the next search
		{
			this.rowKeys.add(0,entry.getKey());
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

	/*
	 *  use scanner to get data
	 */
	private D4mDbResultSet doMatlabQueryOnRows(String rows, String cols) throws CBException, CBSecurityException, TableNotFoundException {
		boolean useBatch= true;
		if(useBatch) {
			return doBatchMatlabQueryOnRows(rows, cols);
		}
		else {
			//			return doScanMatlabQueryOnRows(rows,cols);
		}
		return null;
	}

	//	private D4mDbResultSet doScanMatlabQueryOnRows(String rows, String cols) throws CBException, CBSecurityException, TableNotFoundException {
	//		HashMap<String, String> rowMap = null;
	//		String [] rowsArray = null;
	//		long start = System.currentTimeMillis();
	//
	//		if( this.rowMap == null) {	
	//			this.rowMap = processParam(rows);
	//			rowsArray = (String[])this.rowMap.get("content");
	//			this.rowKeys = param2keys(rowsArray);
	//			rowMap = this.loadRowMap(rows);
	//		}
	//		else {
	//
	//			rowMap = loadRowMap(this.rowMap);
	//
	//			rowsArray = (String[])this.rowMap.get("content");
	//			this.rowKeys = param2keys(rowsArray);
	//
	//		}
	//		D4mDbResultSet results = new D4mDbResultSet();
	//
	//		Range range =null;
	//		if(this.rangesList.size() == 0) {
	//			makeRangesList(rowsArray);
	//		}
	//
	//		if(this.scanner == null || !this.hasNext) {
	//			this.scanner = getScanner();
	//			range = this.rangesList.poll();
	//			this.scanner.setRange(range);
	//			this.scanner.fetchColumnFamily(new Text(this.family));
	//			this.scannerIter = this.scanner.iterator();
	//		}
	//		Entry<Key, Value> entry = iterateMatlabQueryOnRows(scannerIter, rowMap);
	//		this.setRowReturnString(sbRowReturn.toString());
	//		this.setColumnReturnString(sbColumnReturn.toString());
	//		this.setValueReturnString(sbValueReturn.toString());
	//
	//		double elapsed = (System.currentTimeMillis() - start);
	//		results.setQueryTime(elapsed / 1000);
	//		results.setMatlabDbRow(rowList);
	//		this.testResultSet = results;
	//
	//		if(entry == null || !this.hasNext) {
	//			log.debug("Increment index and null startKey.");
	//			index++;
	//			this.startKey = null;
	//		}
	//
	//		//close();
	//		return results;
	//	}
	/*
	 * use BatchScanner to get data
	 */
	private D4mDbResultSet doBatchMatlabQueryOnRows(String rows, String cols) throws CBException, CBSecurityException, TableNotFoundException {

		HashMap<String, String> rowMap = null;

		if( this.rowMap == null) {	
			this.rowMap = processParam(rows);
			String [] rowsArray = (String[])this.rowMap.get("content");
			this.rowKeys = param2keys(rowsArray);
			rowMap = this.loadRowMap(rows);
		}
		else {

			rowMap = loadRowMap(rows);
			if(this.rowKeys == null) {
				String [] rowsArray = (String[])this.rowMap.get("content");
				this.rowKeys = param2keys(rowsArray);
			}
		}
		D4mDbResultSet results = new D4mDbResultSet();
		//ArrayList<D4mDbRow> rowList = new ArrayList<D4mDbRow>();
		//CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);
		BatchScanner scanner =null;
		if(this.bscanner == null) {
			HashSet<Range> ranges = this.loadRanges(this.rowKeys);
			scanner = getBatchScanner();//cbConnection.getBatchScanner(this.tableName, this.numberOfThreads);
			scanner.setRanges(ranges);
			scanner.fetchColumnFamily(new Text(this.family));

		}
		long start = System.currentTimeMillis();

		//Iterator<Entry<Key, Value>> scannerIter = scanner.iterator();
		if(this.scannerIter == null) {
			this.scannerIter = scanner.iterator();
		}
		iterateMatlabQueryOnRows(scannerIter, rowMap);

		if(!this.hasNext) {
			close();
		}
		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(rowList);
		this.testResultSet = results;
		return results;
	}

	private Entry<Key, Value>  iterateMatlabQueryOnRows (Iterator<Entry<Key, Value>> scannerIter, HashMap<String, String> rowMap) {
		String rowKey = null;
		Entry<Key, Value> entry = null;
		while ((this.hasNext =scannerIter.hasNext())) {
			entry = (Entry<Key, Value>) scannerIter.next();
			rowKey = entry.getKey().getRow().toString();
			String column = entry.getKey().getColumnQualifier().toString();//new String(entry.getKey().getColumnQualifier().toString());
			String finalColumn = column;//column.replace(this.family, "");

			if (rowMap.containsKey(rowKey)) {
				String value = new String(entry.getValue().get());
				if(this.buildReturnString(entry.getKey(),rowKey, finalColumn, value)) {
					break;
				}
			}

		}
		return entry;
	}
	private void setNewRowKeyInMap(String rowKey, HashMap<String, Object> contentMap) {
		String[] paramContent = (String[]) contentMap.get("content");
		paramContent[0] = rowKey;
	}
	public D4mDbResultSet doMatlabRangeQueryOnRows(String rows, String cols, String family, String authorizations) throws CBException, CBSecurityException, TableNotFoundException {
		this.family = family;
		connProps.setAuthorizations(authorizations.split(","));
		return doMatlabRangeQueryOnRows(rows, cols);
	}

	private D4mDbResultSet doMatlabRangeQueryOnRows(String rows, String cols) throws CBException, CBSecurityException, TableNotFoundException {
		log.debug("%%%% doMatlabRangeQueryOnRows %%%%");
		HashMap<String, Object> rowMap = null;

		if(this.rowMap == null) {
			rowMap =this.processParam(rows);
			this.rowMap = rowMap;
		}
		else {
			rowMap = this.rowMap;
		}
		String[] rowArray = (String[]) rowMap.get("content");

		HashSet<Range> ranges = new HashSet<Range>();
		if(!this.hasNext) {
			//CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);
			if(this.bscanner == null)
				this.bscanner = getBatchScanner();//cbConnection.getBatchScanner(this.tableName, this.numberOfThreads);

			if (this.getRangeQueryType(rowArray).equals(D4mDbQuery.KEY_RANGE)) {
				// System.out.println("queryType="+this.KEY_RANGE+
				// " rowArray[0]="+rowArray[0]+" rowArray[2]="+rowArray[2]+"<");
				Key startKey = new Key(new Text(rowArray[0]));
				Key endKey = new Key(new Text(rowArray[2]));
				//Range range = new Range(startKey, true, endKey.followingKey(1), false);
				Range range = new Range(startKey, true, endKey.followingKey(PartialKey.ROW), false);
				ranges.add(range);
				bscanner.setRanges(ranges);
				// Note; there is a bug in CB 1.1 for ranges including end key,
				// use "endKey.followingKey(1), false" work around
			}

			if (this.getRangeQueryType(rowArray).equals(D4mDbQuery.POSITIVE_INFINITY_RANGE)) {
				// System.out.println("queryType="+this.POSITIVE_INFINITY_RANGE+
				// " rowArray[0]="+rowArray[0]);
				Key startKey = new Key(new Text(rowArray[0]));
				Range range = new Range(startKey, true, null, true);
				ranges.add(range);
				bscanner.setRanges(ranges);
			}

			if (this.getRangeQueryType(rowArray).equals(D4mDbQuery.NEGATIVE_INFINITY_RANGE)) {
				// System.out.println("queryType="+this.NEGATIVE_INFINITY_RANGE+
				// " rowArray[0]="+rowArray[0]);
				Key endKey = new Key(new Text(rowArray[2]));
				//Range range = new Range(null, true, endKey.followingKey(1), false);
				Range range = new Range(null, true, endKey.followingKey(PartialKey.ROW), false);
				ranges.add(range);
				bscanner.setRanges(ranges);
				// Note; there is a bug in CB 1.1 for ranges including end key,
				// use "endKey.followingKey(1), false" work around
			}

			if (this.getRangeQueryType(rowArray).equals(D4mDbQuery.REGEX_RANGE)) {
				// System.out.println("queryType="+this.REGEX_RANGE+
				// " rowArray[0]="+rowArray[0]);
				String regexParams = this.regexMapper(rowArray[0]);
				bscanner.setRowRegex(regexParams);
				Range range = new Range();
				ranges.add(range);
				bscanner.setRanges(ranges);
			}
		}

		D4mDbResultSet results = new D4mDbResultSet();
		//ArrayList<D4mDbRow> rowList = new ArrayList<D4mDbRow>();
		long start = System.currentTimeMillis();

		if(this.scannerIter == null) {
			this.scannerIter = bscanner.iterator();
		}
		String rowKey=null;
		Entry<Key, Value> entry = iterateOverEntries(this.scannerIter);

		//Set the new row key to start next search
		if(entry != null) {
			setNewRowKeyInMap(entry.getKey().getRow().toString(), this.rowMap);
		}

		// *********************************************
		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(rowList);
		this.testResultSet = results;
		return results;
	}

	public D4mDbResultSet doMatlabQueryOnColumns(String rows, String cols, String family, String authorizations) throws CBException, CBSecurityException, TableNotFoundException {
		this.family = family;
		connProps.setAuthorizations(authorizations.split(","));
		return doMatlabQueryOnColumns(rows, cols);
	}

	private D4mDbResultSet doMatlabQueryOnColumns(String rows, String cols) throws CBException, CBSecurityException, TableNotFoundException {
		log.debug(" <<<< doMatlabQueryOnColumns >>>> ");
		HashMap<?, ?> rowMap = this.loadColumnMap(cols);
		HashMap<String,Object> objColMap = this.processParam(cols);


		String [] colArray = (String []) objColMap.get("content");
		log.debug("LENGTH_OF_COL_ARRAY = "+colArray.length);
		D4mDbResultSet results = new D4mDbResultSet();
		Scanner scanner = null;
		if(this.scanner == null) {
			scanner = getScanner();//cbConnection.getScanner(tableName);
			if( this.startKey != null)
			{
				//Set the range to start search
				this.startRange = new Range(this.startKey,null);
				scanner.setRange(startRange);
			} else {
				this.startRange =  new Range();//new Range(this.startKey,null);
				scanner.setRange(startRange);
			}
			scanner.fetchColumnFamily(new Text(this.family));
		}

		long start = System.currentTimeMillis();

		if(this.scannerIter == null)
			this.scannerIter = scanner.iterator();
		Entry<Key, Value> entry =null;
		while (  (this.hasNext =scannerIter.hasNext())) {

			entry = (Entry<Key, Value>) scannerIter.next();
			String rowKey = entry.getKey().getRow().toString();
			//String column = new String(entry.getKey().getColumnQualifier().toString());
			String finalColumn =  entry.getKey().getColumnQualifier().toString();  //column.replace(this.family, "");
			String value=null;
			boolean isGood=false;
			if( colArray.length == 3 && colArray[1].equals(":")) {
				String colRegex = RegExpUtil.makeRegex(colArray);
				isGood = Pattern.matches(colRegex, finalColumn);
				if(isGood)
					value = new String(entry.getValue().get());
			}
			else {
				if (rowMap.containsValue(finalColumn)) {
					value = new String(entry.getValue().get());
					isGood= true;
				}
			}
			if(isGood)
				if(this.buildReturnString(entry.getKey(),rowKey, finalColumn, value)) {
					break;
				}

		}


		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(rowList);
		this.testResultSet = results;
		return results;
	}

	private Scanner getScanner() throws CBException, CBSecurityException, TableNotFoundException {
		CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);
		if(this.scanner == null)
			this.scanner = cbConnection.getScanner(tableName);
		return scanner;
	}
	private BatchScanner getBatchScanner() throws CBException, CBSecurityException, TableNotFoundException {
		CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);
		if(this.bscanner == null)
			this.bscanner = cbConnection.getBatchScanner(this.tableName, this.numberOfThreads);
		return this.bscanner;
	}

	public void close() {
		this.scannerIter = null;

		if(this.bscanner != null) {
			this.bscanner.close();
			this.bscanner = null;
		}
		if(this.scanner != null) {
			this.scanner = null;
		}
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
		boolean useBatchSearch=false;
		clearBuffers();
		D4mDbResultSet results = null;
		HashMap<String, Object> rowMap = null;
		if( this.rowMap == null) {
			rowMap = this.processParam(rows);
			this.rowMap = rowMap;
		}
		else
			rowMap = this.rowMap;
		HashMap<String, Object> columnMap = null;
		if(this.colMap == null) {
			columnMap =this.processParam(cols);
			this.colMap = columnMap;
		} else
			columnMap = this.colMap;

		results = searchByRowAndColumn(rowMap,columnMap);

		return results;
	}

	private D4mDbResultSet searchByRowAndColumn (HashMap<String, Object> rowMap,HashMap<String, Object> columnMap) {
		D4mDbResultSet results = new D4mDbResultSet();
		HashSet<Range> ranges = null;
		Range range = null;
		String[] rowArray = (String[]) rowMap.get("content");
		String[] columnArray = (String[]) columnMap.get("content");
		int length=rowArray.length;
		String rowkey1 = null;
		String rowkey2 = null;
		boolean rowArrayGood=false;

		long start = System.currentTimeMillis();
		if(length == 1) {
			rowkey1 = rowArray[0];
			rowkey2 = rowArray[0];
			range = makeRange(rowkey1, rowkey2);
			SearchIt(range,columnArray);
			rowArrayGood=true;
		} else if(length == 2 & !rowArray[(length-1)].equals(":")) {
			// Ex, rowQuery='a,c,'
			// Query for rows with 'a' and 'c'
			// Then, we should do use BatchScanner
			// Set up HashSet<Range> ranges
			//HashMap<String, String> loadRowMap(HashMap<String, Object> rowMap)
			//HashMap<String,String> rowMapString = loadRowMap(rowMap);
			ranges = loadRanges(rowArray);
			SearchIt(ranges,columnArray);

			rowArrayGood = true;

		} else 	if(length == 3) {

			if(rowArray[1].equals(":")) {
				rowkey1 = rowArray[0];
				rowkey2 = rowArray[length-1];
			} else if( rowArray[(length-1)].equals(":")) {
				rowkey1 = rowArray[0];
				rowkey2 = rowArray[1];
			}
			log.debug("3__RANGE__"+rowkey1+","+rowkey2);
			range = makeRange(rowkey1, rowkey2);
			SearchIt(range,columnArray);
			rowArrayGood = true;
		}
		if(!rowArrayGood || length > 3) {
			System.out.println("Currently, we cannot handle more than 3 arguments for row key and column key");
			System.out.println("The row and column queries must be in any of the following forms:");
			System.out.println("  *  rowkey (colkey) = \"a,\" ");
			System.out.println("  *  rowkey (colkey) = \"a,b,\"");
			System.out.println("  *  rowkey (colkey) = \"a,:,h,\"");
			System.out.println("  *  rowkey (colkey) = \":\"");

			return results;
		}

		this.setRowReturnString(sbRowReturn.toString());
		this.setColumnReturnString(sbColumnReturn.toString());
		this.setValueReturnString(sbValueReturn.toString());

		double elapsed = (System.currentTimeMillis() - start);
		results.setQueryTime(elapsed / 1000);
		results.setMatlabDbRow(rowList);
		this.testResultSet = results;
		return results;
	}

	public D4mDbResultSet batchSearchByRowAndColumn(HashMap<String, Object> rowMap,HashMap<String, Object> columnMap ,
			String family, String authorizations)  {
		clearBuffers();

		String[] rowArray = (String[]) rowMap.get("content");
		String[] columnArray = (String[]) columnMap.get("content");
		D4mDbResultSet results = new D4mDbResultSet();
		//ArrayList<D4mDbRow> rowList = new ArrayList<D4mDbRow>();
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
		this.testResultSet = results;
		return results;
	}

	private void SearchIt(Range range, String [] columnArray) {
		String colRegex = "";
		if(columnArray.length == 3 && columnArray[1].equals(":")) {
			String colStart1 = columnArray[0].substring(0, 1);
			String colEnd1 = columnArray[2].substring(0,1);
			colRegex = RegExpUtil.makeRegex(columnArray);//"^["+colStart1+"-"+colEnd1+"].*";
			log.debug("COLUMN REGEX="+colRegex);
			SearchIt(range,colRegex);
		} else {

			if(this.scannerIter == null)
				this.scannerIter = getIteratorFromScanner(range);
			Iterator<Entry<Key,Value>> iter = this.scannerIter;
			Entry<Key, Value> entry =null;

			while((this.hasNext=iter.hasNext())) {
				entry = iter.next();
				Key colkey = entry.getKey();

				for(String colKey : columnArray) {
					if(!colKey.equals(":")) {
						String col = colkey.getColumnQualifier().toString();
						boolean isMatching = Pattern.matches(colKey, col);
						if(isMatching) {
							if(buildReturnString(colkey, entry.getValue())) {
								break;
							}
						}
						//	SearchIt(range,colKey);

					}
				}
			}
		}

		if(!this.hasNext) {
			close();
		}
	}

	private Iterator <Entry<Key, Value>> getIteratorFromScanner(Range range) {
		if(this.scannerIter != null)
			return this.scannerIter;
		try {
			this.scanner = getScanner();
			this.scanner.setRange(range);
			//Setup regular expression
			//			String regexName="D4mRegEx";
			//			this.scanner.setScanIterators(1, RegExIterator.class.getName(), regexName);
			//			this.scanner.setScanIteratorOption(regexName, RegExFilter.COLQ_REGEX, colRegex);
			//scanner.setColumnQualifierRegex(colRegex);
			scanner.fetchColumnFamily(new Text(this.family));

		} catch (CBException e) {
			e.printStackTrace();
		} catch (CBSecurityException e) {
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return this.scanner.iterator();
	}
	private void SearchIt(Range range, String colRegex) {
		if(this.scanner == null) {
			try {
				this.scanner = getScanner();
				this.scanner.setRange(range);
				//Setup regular expression
				String regexName="D4mRegEx";
				this.scanner.setScanIterators(1, RegExIterator.class.getName(), regexName);
				this.scanner.setScanIteratorOption(regexName, RegExFilter.COLQ_REGEX, colRegex);
				//scanner.setColumnQualifierRegex(colRegex);
				scanner.fetchColumnFamily(new Text(this.family));

			} catch (CBException e) {
				e.printStackTrace();
			} catch (CBSecurityException e) {
				e.printStackTrace();
			} catch (TableNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		//setup query, set range
		if(this.scannerIter == null) {
			this.scannerIter = this.scanner.iterator();
		}
		iterateOverEntries(this.scannerIter);
	}

	private  Entry<Key, Value> iterateOverEntries(Iterator<Entry<Key, Value>> scannerIter) {
		Entry<Key, Value> entry =null;
		String rowKey = null;
		int count=0;
		while ((this.hasNext =scannerIter.hasNext())) {
			if(this.limit == 0 || this.count < this.limit) {
				entry = (Entry<Key, Value>) scannerIter.next();
				this.startKey = entry.getKey();
				rowKey = entry.getKey().getRow().toString();
				String column = entry.getKey().getColumnQualifier().toString();
				//System.out.println(count+"BEFORE_ENTRY="+rowKey+","+column);
				String value = new String(entry.getValue().get());
				if(this.buildReturnString(entry.getKey(),rowKey, column, value)) {
					break;
				}
				count++;
			}
			else {
				break;
			}
		}
		log.debug("Number of entries = "+count);
		return entry;
	}

	private void SearchIt(HashSet<Range> ranges, String [] columnArray) {
		String colRegex = "";
		if(columnArray.length == 3 && columnArray[1].equals(":")) {
			String colStart1 = columnArray[0].substring(0, 1);
			String colEnd1 = columnArray[2].substring(0,1);
			colRegex = RegExpUtil.makeRegex(columnArray);//"["+colStart1+"-"+colEnd1+"].*";
			SearchIt(ranges,colRegex);
		} else {
			for(String colKey : columnArray) {
				SearchIt(ranges,colKey);
			}
		}

	}
	private void SearchIt(HashSet<Range> ranges, String col) {
		BatchScanner scanner = null;
		Entry<Key, Value> entry =null;
		String rowKey = null;
		try {
			if( this.bscanner == null || !this.hasNext) {
				scanner = getBatchScanner();
				scanner.setRanges(ranges);

				//loop over columns
				//reset column by scanner.clearColumns()s
				//set new columns fetchColumn(fam,col)
				//iterate
				//fill output buffer
				String colRegex = col;
				String regexName="D4mRegEx";
				scanner.setScanIterators(1, RegExIterator.class.getName(), regexName);
				scanner.setScanIteratorOption(regexName, RegExFilter.COLQ_REGEX, colRegex);

				//scanner.setColumnQualifierRegex(colRegex);
				scanner.fetchColumnFamily(new Text(this.family));
			}
			if(scannerIter == null)
				scannerIter = scanner.iterator();
			//int count=0;

			iterateOverEntries(this.scannerIter);

		} catch (CBException e) {
			e.printStackTrace();
		} catch (CBSecurityException e) {
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			setNewRowKeyInMap(rowKey, this.rowMap);
			if(!this.hasNext)
				close();
		}

	}

	private void setFamilyAndAuthorizations(String family, String authorizations) {
		this.family = family;
		connProps.setAuthorizations(authorizations.split(","));
	}

	private void saveTestResults(String rowKey, String columnFamily, String finalColumn, String value) {
		D4mDbRow row = new D4mDbRow();
		row.setRow(rowKey);
		row.setColumnFamily(columnFamily);
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
		return this.limit;
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

	private ArrayList<Key> param2keys(String [] params) {
		ArrayList<Key> keys = new ArrayList<Key> ();
		for(String s : params) {
			if(s.equals(":")) continue;
			Key k = new Key(s);
			keys.add(k);
		}
		return keys;
	}

	public String getColumnReturnString() {
		return this.columnReturnString;
	}

	public void setColumnReturnString(String columnReturnString) {
		this.columnReturnString = columnReturnString;
	}

	public String getRowReturnString() {
		return this.rowReturnString;
	}

	public void setRowReturnString(String rowReturnString) {
		this.rowReturnString = rowReturnString;
	}

	public String getValueReturnString() {
		return this.valueReturnString;
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
			//System.out.println("==>>ROW_ID="+rowId+"<<++");
			if(rowId != null) {
				Key key = new Key(new Text(rowId));
				//Range range = new Range(key, true, key.followingKey(1), false);
				Range range = new Range(key, true, key.followingKey(PartialKey.ROW), false);


				ranges.add(range);
			}
		}
		return ranges;
	}
	public HashSet<Range> loadRanges(String [] rowsRange) {
		HashSet<Range> ranges = new HashSet<Range>();
		//Iterator<String> it = rangeQuery.iterator();
		//	System.out.println("<<< ROW_ARRAY_LENGTH="+rowsRange.length+" >>>");
		int len = rowsRange.length;
		//for (int i = 0; i < len; i++) {
		for (String rowId :rowsRange ) {
			if(rowId != null && !rowId.equals(":")) {
				Key key = new Key(new Text(rowId));
				//Range range = new Range(key, true, key.followingKey(1), false);
				Range range = new Range(key, true, key.followingKey(PartialKey.ROW), false);
				ranges.add(range);
			}
		}
		return ranges;
	}

	public HashSet<Range> loadRanges(ArrayList<Key> rowsRange) {
		HashSet<Range> ranges = new HashSet<Range>();
		for(Key key : rowsRange) {
			Range range = new Range(key, true, key.followingKey(PartialKey.ROW), false);
			ranges.add(range);
		}
		return ranges;

	}

	public Range makeRange(String rowkey1, String rowkey2) {

		Range rng=null;
		rng = new Range(rowkey1,true,rowkey2, true);
		log.debug("RANGE="+rng.toString());
		return rng;
	}

	public void makeRangesList(String [] rowkey) {

		for(String r : rowkey) {
			Range rng = new Range(r);
			this.rangesList.add(rng);
		}
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
			this.sbRowReturn = this.sbRowReturn.delete(0, len);
		len = this.sbColumnReturn.length();
		if(len > 0)
			this.sbColumnReturn = this.sbColumnReturn.delete(0, len);
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
	/*
	 * Get the cumulative total of results retrieved.
	 */
	public long getCumCount() {
		return this.cumCount;
	}
	public void reset() {
		this.startRange = null;
		this.startKey   = null;
		this.rowsQuery = null;
		this.colsQuery = null;
		this.colMap = null;
		this.rowMap = null;
		this.rowStringMap = null;
		this.hasNext=false;
		this.getNext = false;
		this.getAllData = false;
		this.count = 0;
		this.cumCount=0;
		this.rangesList.clear();
		this.rowKeys = null;
		try {
			//Close the BatchScanner
			close();
		} catch(Exception e) {

		} finally {
			this.bscanner = null;
		}

		this.scanner = null;
		this.scannerIter = null;

		clearBuffers();

	}
	public String getFamily() {
		return family;
	}
	public void setFamily(String family) {
		this.family = family;
	}
	public int getNumRows() {
		return numRows;
	}
	public void setNumRows(int numRows) {
		this.numRows = numRows;
	}

	public void setHasNext(boolean hasNext) {
		this.hasNext = hasNext;
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

