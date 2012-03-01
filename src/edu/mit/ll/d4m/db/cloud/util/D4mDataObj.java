/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.util;

import edu.mit.ll.d4m.db.cloud.QueryMethod;
/**
 * @author cyee
 *
 */
public class D4mDataObj {

	private String row=null;
	private String colFamily=null;
	private String colQualifier=null;
	private String value = null;
	private QueryMethod method;  //Query switch
	
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

	public QueryMethod getMethod() {
		return method;
	}

	public void setMethodName(QueryMethod methodName) {
		this.method = methodName;
	}

}
