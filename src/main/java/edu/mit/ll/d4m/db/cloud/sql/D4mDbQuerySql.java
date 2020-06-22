/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.apache.log4j.Logger;

/**
 * Class to execute a SQL query and hold results as string [row, column, value]
 *  
 *  
 *  *Create db connection
 *  *Execute query
 *  *Iterate over ResultSet
 * 
 * @author CHV8091
 *
 */
public class D4mDbQuerySql {
    private static final Logger log = Logger.getLogger(D4mDbQuerySql.class);
    private String rows = new String();
    private String cols = new String();
    private String vals = new String();
    private Connection conn=null;

    /*
     * Make DB connection
     */
    public D4mDbQuerySql(String url, String user, String pword) {
        super();
        try {
            conn = DriverManager.getConnection(url, user, pword);
        } catch (SQLException e) {
            log.warn("",e);
        }

    }
    public D4mDbQuerySql() {


    }

    public void executeQuery( String sqlQuery) {
        Statement st;
        try {
            st = this.conn.createStatement();
            ResultSet rs = st.executeQuery(sqlQuery);
            extractRowsColumnsValues(rs);
        } catch (SQLException e) {
            log.warn("",e);
        }

    }
    
    private void extractRowsColumnsValues(ResultSet rs) throws SQLException {
        StringBuffer sbVals= new StringBuffer();
        StringBuffer sbCols= new StringBuffer();
        StringBuffer sbRows= new StringBuffer();
        ResultSetMetaData rsmd = rs.getMetaData();
        int colCount= rsmd.getColumnCount();
        int rowCount=1;
        while(rs.next()) {
            rowCount = rs.getRow();
            for(int i = 0; i < colCount; i++) {
                String c = rsmd.getColumnName(i);
                String v = rs.getString(i);
                if(v == null) {
                    sbVals.append("NULL");
                } else {
                    sbVals.append( v.replace("\n", ""));
                }
                sbVals.append("\n");  
                sbCols.append(c).append("\n");
                sbRows.append(Integer.toString(rowCount));
            }
        }

        vals = sbVals.toString();
        cols = sbCols.toString();
        rows = sbRows.toString();
    }
    public Connection getConn() {
        return conn;
    }
    public void setConn(Connection conn) {
        this.conn = conn;
    }
    public String getVals() {
        return vals;
    }
    public void setVals(String vals) {
        this.vals = vals;
    }
    public String getRows() {
        return rows;
    }
    public void setRows(String rows) {
        this.rows = rows;
    }
    public String getCols() {
        return cols;
    }
    public void setCols(String cols) {
        this.cols = cols;
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
