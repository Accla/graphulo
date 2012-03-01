/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.util;

import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.mit.ll.d4m.db.cloud.D4mDbQuery;
import edu.mit.ll.d4m.db.cloud.QueryMethod;

/**
 *  Static methods
 * @author cyee
 *
 */
public class D4mQueryUtil {

	private static Logger log = Logger.getLogger(D4mQueryUtil.class);


	public static D4mDataObj whatQueryMethod (String rows, String cols) {

		D4mDataObj dataObj= new D4mDataObj();
		dataObj.setRow(rows);
		dataObj.setColQualifier(cols);

		if ((!rows.equals(":")) && (cols.equals(":"))) {

			HashMap<String, Object> rowMap = processParam(rows);
			//this.rowMap = rowMap;
			String[] paramContent = (String[]) rowMap.get("content");
			// System.out.println("this.isRangeQuery(paramContent)="+this.isRangeQuery(paramContent));
			if (isRangeQuery(paramContent)) {
				log.debug("MATLAB_RANGE_QUERY_ON_ROWS");
				dataObj.setMethodName(QueryMethod.MATLAB_RANGE_QUERY_ON_ROWS);
				//return this.doMatlabRangeQueryOnRows(rows, cols);
			}
			else {
				log.debug("MATLAB_QUERY_ON_ROWS");
				dataObj.setMethodName(QueryMethod.MATLAB_QUERY_ON_ROWS);
				//				return this.doMatlabQueryOnRows(rows, cols);
			}
		} else if ((rows.equals(":")) && (!cols.equals(":"))) {
			log.debug("MATLAB_QUERY_ON_COLS");
			dataObj.setMethodName(QueryMethod.MATLAB_QUERY_ON_COLS);
			//			return this.doMatlabQueryOnColumns(rows, cols);
		} else if ((rows.equals(":")) && (cols.equals(":"))) {
			log.debug("GET_ALL_DATA");
			dataObj.setMethodName(QueryMethod.GET_ALL_DATA);
			//		return this.getAllData();
		} else if( (!rows.startsWith(":") && !rows.equals(":") ) && (!cols.startsWith(":")) && (!cols.equals(":")) ) {
			log.debug("SEARCH_BY_ROW_&_COL");
			dataObj.setMethodName(QueryMethod.SEARCH_BY_ROW_AND_COL);
			//	return this.searchByRowAndColumn(rows, cols, null,null);
		} else {
			//AssocColumnWithRow
			dataObj.setMethodName(QueryMethod.ASSOC_COLUMN_WITH_ROW);

		}


		return dataObj;
	}
	public static HashMap<String, String> assocColumnWithRow(String rows, String cols) {

		HashMap<String, Object> rowMap = processParam(rows);
		HashMap<String, Object> columnMap = processParam(cols);
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
	 *  
	 *   queryString   string   eg a comma-delimited list cat,:,rat,
	 */
	private static HashMap<String, String>  loadMap(String queryString) {
		HashMap<String, Object> tmpObjMap = processParam(queryString);
		String[] contentArray = (String[]) tmpObjMap.get("content");
		HashMap<String, String> resultMap = loadMap(contentArray);
		return resultMap;

	}

	private static HashMap<String,String> loadMap (String [] contentArray) {
		HashMap<String, String> resultMap = new HashMap<String, String>();
		for (int i = 0; i < contentArray.length; i++) {
			resultMap.put(contentArray[i], contentArray[i]);
		}
		return resultMap;
	}
	public static HashMap<String, String> loadColumnMap(String cols) {

		HashMap<String, String> resultMap = loadMap(cols);
		return resultMap;
	}

	public static HashMap<String, String> loadColumnMap(String [] cols) {
		HashMap<String, String> resultMap = loadMap(cols);
		return resultMap;
	}

	public static HashMap<String, String> loadColumnMap(HashMap<String,Object> colMap) {
		String[] contentArray = (String[]) colMap.get("content");
		HashMap<String,String> resultMap = loadMap(contentArray);
		return resultMap;
	}

	public static HashMap<String, String> loadRowMap(String rows) {

		HashMap<String, String> resultMap = loadMap(rows);
		return resultMap;
	}

	public static HashMap<String, String> loadRowMap(HashMap<String, Object> rowMap) {

		String[] rowArray = (String[]) rowMap.get("content");
		HashMap<String, String> resultMap = loadMap(rowArray);
		return resultMap;
	}

	public static boolean isRangeQuery(String[] paramContent) {
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

	public static String getRangeQueryType(String[] paramContent) {
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

	public static HashMap<String, Object> processParam(String param) {
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

}
