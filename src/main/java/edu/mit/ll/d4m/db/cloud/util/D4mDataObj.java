/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.util;

import java.util.ArrayList;

import edu.mit.ll.d4m.db.cloud.D4mDbRow;
import edu.mit.ll.d4m.db.cloud.QueryMethod;
/**
 * @author cyee
 *
 */
public class D4mDataObj {

	private String row=null;
	private String colFamily="";
	private String colQualifier=null;
	private String value = null;
	private QueryMethod queryMethod;  //Query switch
	private ArrayList<D4mDbRow> rowList=null;
	private double queryTime = 0; // seconds, elapsed time to execute query

	/**
	 * 
	 */
	public D4mDataObj() {
	}

	public String getRow() {
		return row;
	}

	public void setRow(String row) {
		this.row = row;
	}

	public String getColFamily() {
		return colFamily;
	}

	public void setColFamily(String colFamily) {
		this.colFamily = colFamily;
	}

	public String getColQualifier() {
		return colQualifier;
	}

	public void setColQualifier(String colQualifier) {
		this.colQualifier = colQualifier;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public QueryMethod getQueryMethod() {
		return queryMethod;
	}

	public void setQueryMethod(QueryMethod methodName) {
		this.queryMethod = methodName;
	}

	public ArrayList<D4mDbRow> getRowList() {
		return rowList;
	}

	public void setRowList(ArrayList<D4mDbRow> rowList) {
		this.rowList = rowList;
	}

	public String toString() {
		return "QUERY_METHOD:"+ this.queryMethod+", ROW=["+
		this.row+"], FAMILY=["+ this.colFamily+"], QUALIFIER=["+ this.colQualifier+"], VALUE=["+ this.value+"]";
	}
	/**
	 * Returns a String with the rows, column qualifiers, and values held in this data object in a nice tabular format.
	 * @return The printed string.
	 */
	public String prettyPrint() {
		String[] rowsArr = D4mQueryUtil.processParam(row);
		String[] colsArr = D4mQueryUtil.processParam(colQualifier);
		String[] valsArr = D4mQueryUtil.processParam(value);
		if (rowsArr == null || colsArr == null || valsArr == null)
			return "";
		assert rowsArr.length == colsArr.length && valsArr.length == colsArr.length;
		
		StringBuffer[] bufs = new StringBuffer[rowsArr.length]; // one for each line
		int maxRow = 0;
		for (int i = 0; i < bufs.length; i++) {
			maxRow = rowsArr[i].length() > maxRow ? rowsArr[i].length() : maxRow;
			bufs[i] = new StringBuffer(rowsArr[i]);
		}
		maxRow++; // extra space between columns
		int maxCol = 0;
		for (int i = 0; i < bufs.length; i++) {
			for (int j = maxRow - bufs[i].length(); j > 0; j--)
				bufs[i].append(' ');
			bufs[i].append(colsArr[i]);
			maxCol = bufs[i].length() > maxCol ? bufs[i].length() : maxCol;
		}
		maxCol++;
		StringBuilder result = new StringBuilder(maxCol*bufs.length);
		for (int i = 0; i < bufs.length; i++) {
			result.append(bufs[i]);
			for (int j = maxCol - bufs[i].length(); j > 0; j--)
				result.append(' ');
				//bufs[i].append(' ');
			result.append(valsArr[i]).append('\n');
			//bufs[i].append(valsArr[i]);
		}
		return result.toString();
	}

	public double getQueryTime() {
		return queryTime;
	}

	public void setQueryTime(double queryTime) {
		this.queryTime = queryTime;
	}
}
