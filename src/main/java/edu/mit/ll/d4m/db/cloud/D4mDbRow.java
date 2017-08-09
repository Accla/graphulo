package edu.mit.ll.d4m.db.cloud;

/**
 * d4m database row
 *
 * This is strictly a storage class.  While named "row", it appears that is best thought of as
 * holding a quad of row id, column, columnFamily and value.
 * @author wi20909
 */
public class D4mDbRow {
	private String row = "";
	private String column = "";
	private String columnFamily="";
	private String value = "";

	public D4mDbRow() {
	}
	
	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getRow() {
		return row;
	}

	public void setRow(String row) {
		this.row = row;
	}

	public String toString() {
		String r=" "+ this.row+","+this.columnFamily+","+this.column+","+this.value;
		return r;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
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
