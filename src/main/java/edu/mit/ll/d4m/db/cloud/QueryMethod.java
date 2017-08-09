package edu.mit.ll.d4m.db.cloud;

public enum QueryMethod {

		 GET_ALL_DATA ( "GET_ALL_DATA"),
		 MATLAB_QUERY_ON_COLS ("MATLAB_QUERY_ON_COLS"),
		 MATLAB_RANGE_QUERY_ON_ROWS ("MATLAB_RANGE_QUERY_ON_ROWS"),
		 MATLAB_QUERY_ON_ROWS ("MATLAB_QUERY_ON_ROWS"),
		 SEARCH_BY_ROW_AND_COL ( "SEARCH_BY_ROW_AND_COL"),
		 ASSOC_COLUMN_WITH_ROW ( "AssocColumnWithRow");

		 private String name=null;
		 QueryMethod(String name) {
			 this.name= name;
		 }
		 public String getName() {
			 return this.name;
		 }
	

}
