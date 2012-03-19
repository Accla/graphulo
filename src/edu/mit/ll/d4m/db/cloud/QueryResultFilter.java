/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import edu.mit.ll.d4m.db.cloud.util.D4mDataObj;
import edu.mit.ll.d4m.db.cloud.util.D4mQueryUtil;
import edu.mit.ll.d4m.db.cloud.util.RegExpUtil;

/**
 * The QueryResultController has to decide if the result fits the query.
 * It does the filtering.
 * 
 * @author cyee
 *
 */
public class QueryResultFilter {
	private static String ME="QueryResultFilter";
	private static Logger log = Logger.getLogger(QueryResultFilter.class);

	private String rows = null;
	private String cols = null;

	private String family = null;
	HashMap<String, Object> objectMap=null;
	HashMap<String, Object> colObjectMap=null;

	HashMap<String,String> stringMap = null;

	private StringBuilder sbRowReturn = new StringBuilder();
	private StringBuilder sbColumnReturn = new StringBuilder();
	private StringBuilder sbValueReturn = new StringBuilder();

	//	private final String GET_ALL_DATA = "GET_ALL_DATA";
	//	private final String MATLAB_QUERY_ON_COLS = "MATLAB_QUERY_ON_COLS";
	//	private final String MATLAB_RANGE_QUERY_ON_ROWS = "MATLAB_RANGE_QUERY_ON_ROWS";
	//	private final String MATLAB_QUERY_ON_ROWS = "MATLAB_QUERY_ON_ROWS";
	//	private final String SEARCH_BY_ROW_AND_COL = "SEARCH_BY_ROW_AND_COL";
	//	private final String ASSOC_COLUMN_WITH_ROW = "AssocColumnWithRow";
	private QueryMethod METHOD=QueryMethod.GET_ALL_DATA;
	private Pattern pattern = null;
	private Pattern pattern2 = null;
	private boolean hasData = false;
	private int count =0;
	ArrayList<D4mDbRow> rowList = new ArrayList<D4mDbRow>();  //for testing, Set Log level to INFO to trigger
	long timeLastUpdated = -1l;
	/**
	 * 
	 */
	public QueryResultFilter() {


	}

	public void init(String rows, String cols, QueryMethod methName) {
		reset();
		this.rows = rows;
		this.cols = cols;

		this.METHOD = methName;
		switch(this.METHOD) {
		case GET_ALL_DATA:

			break;
		case MATLAB_QUERY_ON_COLS:
			setupMatlabQueryOnColumns(rows, cols);
			break;
		case MATLAB_RANGE_QUERY_ON_ROWS:
			setupMatlabRangeQueryOnRows(rows, cols);

			break;
		case MATLAB_QUERY_ON_ROWS:
			setupMatlabQueryOnRows(rows, cols);
			break;
		case SEARCH_BY_ROW_AND_COL:
			setupSearchByRowAndColumn(rows, cols);
			break;
		case ASSOC_COLUMN_WITH_ROW:
			setupAssocColumnWithRow(rows,cols);
			break;

		default:
			break;

		}



	}
	public void init(String rows, String cols) {
		resetAll();
		this.rows = rows;
		this.cols = cols;

		//Determine the type of query - getAllData, matlabQueryOnColumn, MatlabRangeQueryOnRows, MATLAB_QUERY_ON_ROWS, SEARCH_BY_ROW_&_COL


		if ((!rows.equals(":")) && (cols.equals(":"))) {

			HashMap<String, Object> rowMap = D4mQueryUtil.processParam(rows);
			this.objectMap = rowMap;
			String[] paramContent = (String[]) rowMap.get("content");
			// System.out.println("this.isRangeQuery(paramContent)="+this.isRangeQuery(paramContent));
			if (D4mQueryUtil.isRangeQuery(paramContent)) {
				log.debug("MATLAB_RANGE_QUERY_ON_ROWS");
				this.METHOD=QueryMethod.MATLAB_RANGE_QUERY_ON_ROWS;
				setupMatlabRangeQueryOnRows(rows, cols);
				//	return this.doMatlabRangeQueryOnRows(rows, cols);
			} else {
				log.debug("MATLAB_QUERY_ON_ROWS");
				this.METHOD=  QueryMethod.MATLAB_QUERY_ON_ROWS;
				//return this.doMatlabQueryOnRows(rows, cols);
				setupMatlabQueryOnRows(rows, cols);
			}
		} else if ((rows.equals(":")) && (!cols.equals(":"))) {
			log.debug("MATLAB_QUERY_ON_COLS");
			this.METHOD=QueryMethod.MATLAB_QUERY_ON_COLS;
			//return this.doMatlabQueryOnColumns(rows, cols);
			setupMatlabQueryOnColumns(rows, cols);
		} else if ((rows.equals(":")) && (cols.equals(":"))) {
			log.debug("GET_ALL_DATA");
			this.METHOD=  QueryMethod.GET_ALL_DATA;
			//return this.getAllData();
		} else if( !rows.startsWith(":") && !rows.equals(":")
				&& (!cols.startsWith(":")) && (!cols.equals(":")) ) {

			log.debug("SEARCH_BY_ROW_&_COL");
			this.METHOD=QueryMethod.SEARCH_BY_ROW_AND_COL;
			setupSearchByRowAndColumn(rows, cols);
		} else  {
			this.METHOD= QueryMethod.ASSOC_COLUMN_WITH_ROW;
			setupAssocColumnWithRow(rows,cols);
		}

		if(log.isDebugEnabled()) {
			String message="^*^*QUERY_METHOD="+this.METHOD.toString();
			log.debug(message);
			System.out.println(message);
		}


	}

	private void setupMatlabRangeQueryOnRows(String rows, String cols) {
		return;
	}
	private void setupMatlabQueryOnRows(String rows, String cols) {
		this.objectMap = D4mQueryUtil.processParam(rows);
		String [] rowArray = (String[]) objectMap.get("content");
		String regex = RegExpUtil.makeRegex(rowArray);
		this.pattern = Pattern.compile(regex);
		return;
	}

	private void setupMatlabQueryOnColumns(String rows, String cols) {
		this.objectMap = D4mQueryUtil.processParam(cols);
		String [] contentArray = (String[]) objectMap.get("content");
		String regex = RegExpUtil.makeRegex(contentArray);
		this.pattern = Pattern.compile(regex);
	}
	private void setupSearchByRowAndColumn(String rows, String cols) {
		objectMap=D4mQueryUtil.processParam(cols); 
		String [] contentArray2 = (String[]) objectMap.get("content");
		String regex2 = RegExpUtil.makeRegex(contentArray2);
		this.pattern2 = Pattern.compile(regex2);

	}

	private void setupAssocColumnWithRow(String rows, String cols) {
		HashMap<String, Object> objectMap1=D4mQueryUtil.processParam(rows); 
		String [] contentArray = (String[]) objectMap1.get("content");
		String regex = RegExpUtil.makeRegex(contentArray);
		this.pattern = Pattern.compile(regex);
		HashMap<String, Object> objectMap2=D4mQueryUtil.processParam(cols); 
		String [] contentArray2 = (String[]) objectMap2.get("content");
		String regex2 = RegExpUtil.makeRegex(contentArray2);
		this.pattern2 = Pattern.compile(regex2);

	}

	public void getAllData(D4mDataObj keyObj) {
		log.debug("getAllData");
		buildStringReturn(keyObj.getRow(), keyObj.getColFamily(),keyObj.getColQualifier(), keyObj.getValue());
	}

	public void assocColumnWithRow(D4mDataObj keyObj) {
		log.debug("=== assocColumnWithRow ===");
		String row = keyObj.getRow();
		String col = keyObj.getColQualifier().replace(keyObj.getColFamily(), "");
		Matcher mat1 = this.pattern.matcher(row);
		Matcher mat2 = this.pattern2.matcher(col);
		if(mat1.matches() && mat2.matches()) {
			buildStringReturn(keyObj.getRow(), keyObj.getColFamily(),keyObj.getColQualifier(), keyObj.getValue());
		}
	}

	public void matlabQueryOnCols(D4mDataObj keyObj) {
		log.debug("=== matlabQueryOnCols ===");
		String col = keyObj.getColQualifier();
		Matcher match = this.pattern.matcher(col);
		if(log.isDebugEnabled()) {
			//System.out.println(ME+" === matlabQueryOnCols ===");

//			System.out.println(ME+"  COL = "+col + " to match PATTERN="+this.pattern.pattern());
		}


		if(match.matches()) {
			this.buildStringReturn(keyObj.getRow(), keyObj.getColFamily(), keyObj.getColQualifier(), keyObj.getValue());
		} else {
			String [] contentArray = (String[]) objectMap.get("content");
			if(D4mQueryUtil.isWithInRange(col, contentArray)) {
			
				//if(col.compareTo(contentArray[0]) > 0 && col.compareTo(contentArray[2]) < 0) {
					this.buildStringReturn(keyObj.getRow(),
							keyObj.getColFamily(), 
							keyObj.getColQualifier(),
							keyObj.getValue());
			
				//}
		}
		}

	}

	public void matlabQueryOnRows(D4mDataObj keyObj) {
		String rowkey = keyObj.getRow();
		if(log.isDebugEnabled()) {
			log.debug("=== matlabQueryOnRows ===");
			//System.out.println(ME+" === matlabQueryOnRows ===");
		//	System.out.println(ME+"  ROW_KEY = "+rowkey + " to match PATTERN="+this.pattern.pattern());
		}

		Matcher match = this.pattern.matcher(rowkey);
		log.debug("MATCH RESULT="+keyObj+" <---> PATTERN ="+pattern.pattern());
		if(match.matches()) {
			this.buildStringReturn(rowkey,keyObj.getColFamily(), keyObj.getColQualifier(), keyObj.getValue());

		}


	}
	public void matlabRangeQueryOnRows(D4mDataObj keyObj) {
		log.debug("=== matlabRangeQueryOnRows ===");
		buildStringReturn(keyObj.getRow(), keyObj.getColFamily(),keyObj.getColQualifier(), keyObj.getValue());
	}

	public void searchByRowAndColumn(D4mDataObj keyObj) {
		log.debug("=== searchByRowAndColumn ===");
		this.hasData=false;
		String col = keyObj.getColQualifier().replace(keyObj.getColFamily(),"");
		Matcher mat = this.pattern2.matcher(col);
		if(mat.matches()) {
			buildStringReturn(keyObj.getRow(), keyObj.getColFamily(),keyObj.getColQualifier(), keyObj.getValue());
			this.hasData=true;
		} else {
			String [] contentArray = (String[]) objectMap.get("content");
			if(D4mQueryUtil.isWithInRange(col, contentArray)) {
				buildStringReturn(keyObj.getRow(), 
						keyObj.getColFamily(),
						keyObj.getColQualifier(),
						keyObj.getValue());

			}
		}
	}

	public void buildStringReturn(String rowKey, String family, String columnQualifier, String value) {
		this.sbRowReturn.append(rowKey + D4mDbQuery.newline);
		this.sbColumnReturn.append(columnQualifier.replace(family, "") + D4mDbQuery.newline);
		this.sbValueReturn.append(value + D4mDbQuery.newline);
		this.hasData=true;
		this.count++;
		this.timeLastUpdated = System.currentTimeMillis();
		if(D4mConfig.DEBUG) {
			saveTestResults(rowKey,family, columnQualifier, value);
		}
	}
	private void saveTestResults(String rowKey, String columnFamily, String finalColumn, String value) {
		D4mDbRow row = new D4mDbRow();
		row.setRow(rowKey);
		row.setColumnFamily(columnFamily);
		row.setColumn(finalColumn);
		row.setValue(value);
		this.rowList.add(row);
	}

	public void reset() {
		//this.pattern = null;
		//this.stringMap =null;
		//this.objectMap = null;
		this.rowList.clear();
		clearBuffers();
		this.count =0;
		this.hasData=false;
		this.timeLastUpdated = System.currentTimeMillis();
	}
	public void resetAll() {

		this.pattern    = null;
		this.stringMap  = null;
		this.objectMap  = null;

		reset();

	}
	/*
	 * Clear StringBuffers for reuse
	 */
	public void clearBuffers() {
		int len = this.sbRowReturn.length();
		if(len > 0)
			this.sbRowReturn = this.sbRowReturn.delete(0, len);
		len = this.sbColumnReturn.length();
		if(len > 0)
			this.sbColumnReturn = this.sbColumnReturn.delete(0, len);
		len = this.sbValueReturn.length();
		if(len > 0)
			this.sbValueReturn = this.sbValueReturn.delete(0, len);

	}

	public boolean hasData() {
		return this.hasData;
	}
	public int getCount() {
		return this.count;
	}
	/**
	 * @param key  new result
	 * @param appendResults  append result to existing results, 
	 *                       true to append, false to clear the buffer
	 */
	public void query(D4mDataObj key, boolean appendResults) {
		this.hasData=false;
		if(!appendResults) clearBuffers();

		if(log.isDebugEnabled()) {
			log.debug(" INCOMING RESULT TO CHECK === "+key.toString());
			//System.out.println(" INCOMING RESULT TO CHECK === "+key.toString());
		}
		switch(this.METHOD)  {
		case GET_ALL_DATA:
			getAllData(key);
			break;
		case MATLAB_RANGE_QUERY_ON_ROWS:
			matlabRangeQueryOnRows(key);
			break;
		case MATLAB_QUERY_ON_ROWS:
			matlabQueryOnRows(key);
			break;
		case MATLAB_QUERY_ON_COLS:
			matlabQueryOnCols(key);
			break;
		case SEARCH_BY_ROW_AND_COL:
			searchByRowAndColumn(key);
			break;
		case ASSOC_COLUMN_WITH_ROW:
			assocColumnWithRow(key);
			break;
		default:
			break;
		}

		if(log.isDebugEnabled()) {
		//	System.out.println(this.count+")  ROW RESULTS IN S-BUFFER => "+this.getRowResult());
		}
	}

	public String getRowResult() {
		return this.sbRowReturn.toString();
	}
	public String getColumnResult() {
		return this.sbColumnReturn.toString();
	}
	public String getValueResult() {
		return this.sbValueReturn.toString();
	}

	public ArrayList<D4mDbRow> getRowList() {
		return rowList;
	}

	public void setRowList(ArrayList<D4mDbRow> rowList) {
		this.rowList = rowList;
	}

	public long getTimeLastUpdated() {
		return timeLastUpdated;
	}

	public void setTimeLastUpdated(long timeLastUpdated) {
		this.timeLastUpdated = timeLastUpdated;
	}


	//	public enum QueryMethod {
	//		 GET_ALL_DATA ( "GET_ALL_DATA"),
	//		 MATLAB_QUERY_ON_COLS ("MATLAB_QUERY_ON_COLS"),
	//		 MATLAB_RANGE_QUERY_ON_ROWS ("MATLAB_RANGE_QUERY_ON_ROWS"),
	//		 MATLAB_QUERY_ON_ROWS ("MATLAB_QUERY_ON_ROWS"),
	//		 SEARCH_BY_ROW_AND_COL ( "SEARCH_BY_ROW_AND_COL"),
	//		 ASSOC_COLUMN_WITH_ROW ( "AssocColumnWithRow");
	//
	//		 private String name=null;
	//		 QueryMethod(String name) {
	//			 this.name= name;
	//		 }
	//		 public String getName() {
	//			 return this.name;
	//		 }
	//	}
}
/*
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % D4M: Dynamic Distributed Dimensional Data Model 
 * % MIT Lincoln Laboratory
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % (c) <2010>  Massachusetts Institute of Technology
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 */

