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
		String s = "QUERY_METHOD:"+this.queryMethod+", ROW=["+
		this.row+"], FAMILY=["+this.colFamily+"], QUALIFIER=["+this.colQualifier+"], VALUE=["+this.value+"]";
		return s;
	}

	public double getQueryTime() {
		return queryTime;
	}

	public void setQueryTime(double queryTime) {
		this.queryTime = queryTime;
	}
}
