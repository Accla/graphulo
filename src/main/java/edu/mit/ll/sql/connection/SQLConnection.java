package edu.mit.ll.sql.connection;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;


public class SQLConnection {

	private static final Logger logger = Logger.getLogger(SQLConnection.class);
	public String databaseAddress = "jdbc:mysql://localhost:3306/UnknownDb";
	public String user = "user";
	public String password = "pass";
	public String driverName = "org.mysql.jdbc.Driver";

	private Statement statement;

	protected Connection dbConnection;
	
	/**
	 * Construct the SQLConnection object with values read from sql.properties file
	 */
	public SQLConnection() {
		try{
        this.driverName = (String) SQLProperties.get("driver");	
		this.databaseAddress = (String) SQLProperties.get("address");
        this.user = (String) SQLProperties.get("username");
        this.password = (String) SQLProperties.get("password");
		}
		catch(Exception ex){
			logger.error("Could not read properties file: " + ex.getMessage());
		}
	}

	/**
	 * Construct the SQLConnection object
	 * @param driverName
	 * @param databaseAddress
	 * @param userName
	 * @param password
	 */
	public SQLConnection(String driverName, String databaseAddress, String userName, String password) {
		if (driverName != null)
			this.driverName = driverName;

		if (databaseAddress != null)
			this.databaseAddress = databaseAddress;

		if (userName != null)
			this.user = userName;

		if (password != null)
			this.password = password;
	}
	



	/**
	 * Initiate a connection with the database -- throw a SQLException if error
	 * occurs. If error occurs, the connection is null.
	 */
	public Connection connect() throws SQLException {
		logger.info("Connecting to " + databaseAddress + " with user " + user + " and password " + password);
		dbConnection = null;

		try {
			// Load jdbc driver
			Class.forName(driverName);

			// Connect to database (using DriverManager)
			dbConnection = DriverManager.getConnection(databaseAddress, user, password);
		}
		catch (ClassNotFoundException e) {
			logger.error("Could not find driver: " + driverName, e);
			dbConnection = null;
		}

		return dbConnection;
	}

	/**
	 * Return current connection to database (without attempting to connect).
	 */
	public Connection getConnection() {
		return dbConnection;
	}

	/**
	 * Closes connection to database.
	 */
	public void close() {
		try {
			if (dbConnection != null)
				dbConnection.close();
		}
		catch (Exception e) {
			logger.error(e);
		}
		finally {
			dbConnection = null;
		}
	}

	/**
	 * executeQuery
	 * @param sql:SQL statement to be sent to the database, typically a static SQL SELECT statement
	 * @return Resultset:object that contains the data produced by the given query; never null
	 * @throws SQLException: if a database access error occurs or the given SQL statement produces anything other than a single ResultSet object
	 */
	public ResultSet executeQuery(String sql) throws SQLException {
		Statement statement = (Statement) dbConnection.createStatement();
		ResultSet ret = (ResultSet) statement.executeQuery(sql);
		statement.close();
		return ret;
	}
	
	/**
	 * executeQuery_String
	 * @param sql:SQL statement to be sent to the database, typically a static SQL SELECT statement
	 * @return String:String that contains the data produced by the given query; never null
	 */
	public String exectueQuery_String(String sql)  {
        StringBuilder sb = new StringBuilder();
		try{
	        ResultSet rs = executeQuery(sql);
        	//get the number of columns
	        ResultSetMetaData rsmd = rs.getMetaData(); 
	        int numColumns = rsmd.getColumnCount();
	        sb.append("{");
	        while (rs.next())
	        {
	        	for (int i=1; i< numColumns; i++) {
	        		String colValue = rs.getString(i);
	        		sb.append(colValue + ",");
	        	}
	        	sb.append(";");
	        }
	        //return sb.toString();
	        sb.append("}");
		}
		catch (SQLException ex)
		{
			logger.error("Unable to execute query: " + ex.getMessage());
		}

		return sb.toString();

	}

	/**
	 * executeUpdate
	 * @param sql: an SQL INSERT, UPDATE or DELETE statement or an SQL statement that returns nothing
	 * @return int: either the row count for INSERT, UPDATE or DELETE statements, or 0 for SQL statements that return nothing
	 * @throws SQLException: if a database access error occurs or the given SQL statement produces a ResultSet object
	 */
	public int executeUpdate(String sql) throws SQLException {
		Statement statement = (Statement) dbConnection.createStatement();
		int ret = statement.executeUpdate(sql);
		statement.close();
		return ret;
	}

	
	/**
	 * executeUpdate_int
	 * @param sql: an SQL INSERT, UPDATE or DELETE statement or an SQL statement that returns nothing
	 * @return int: either the row count for INSERT, UPDATE or DELETE statements, or 0 for SQL statements that return nothing -1 for error
	 */
	public int exectueUpdate_int(String sql)  {

		int retval = -1;
		try{
	        retval = executeUpdate(sql);
		}
		catch (SQLException ex)
		{
			logger.error("Unable to execute update query: " + ex.getMessage());
		}

		return retval;

	}
	
	/**
	 * Executes the given SQL statement, which may return multiple results. 
	 * In some (uncommon) situations, a single SQL statement may return multiple result sets and/or update counts. 
	 * Normally you can ignore this unless you are (1) executing a stored procedure that you know may return multiple results or 
	 * (2) you are dynamically executing an unknown SQL string.
	 * The execute method executes an SQL statement and indicates the form of the first result. 
	 * You must then use the methods getResultSet or getUpdateCount to retrieve the result, and getMoreResults to move to any subsequent result(s).
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public Object exectue(String sql) throws SQLException {
		if (statement != null) {
			statement.close();
		}
		statement = (Statement) dbConnection.createStatement();
		boolean ret = statement.execute(sql);

		if (ret)
			return (ResultSet) statement.getResultSet();
		else
			return statement.getUpdateCount();
	}

	/**
	 * Create a table with given schema. If schema is empty it creates and empty table
	 * @param tableName
	 * @param schema
	 * @throws SQLException
	 */
	public void createTable(String tableName, String schema) throws SQLException{
		try {
		    Statement stmt = dbConnection.createStatement();
	    	String sql = "CREATE TABLE " + tableName + schema ;
		    stmt.executeUpdate(sql);
		} catch (SQLException e) {
		}
		
	}

	/**
	 * Drop a Table
	 * @param tableName: Table to delete
	 * @throws SQLException
	 */
	public void deleteTable(String tableName) throws SQLException {
		try {
		    Statement stmt = dbConnection.createStatement();
		    
		    stmt.executeUpdate("DROP TABLE " + tableName);
		} catch (SQLException e) {
		}
		
	}

	/**
	 * Returns a List of Tables in the Database 
	 * @return
	 * @throws SQLException
	 */
	public ResultSet getTables() throws SQLException {
		DatabaseMetaData dbmd = dbConnection.getMetaData();

		String[] types = { "TABLE" };
		ResultSet resultSet = dbmd.getTables(null, null, "%", types);
		return resultSet;

	}
	
	
	
    /**
     * Get a string containing Table Names of all the tables
     * @return String containing Table Names of all the tables
     */
    public String getTables_String() 
    {
    	StringBuilder sb = new StringBuilder();
    	try {
    		ResultSet res = getTables();

    		while(res.next()){
    			String tableName = res.getString(3);
    			sb.append(tableName + " ");
    		}
    	}
    	catch (SQLException ex){
    		logger.error("Unable to get list of Tables: " + ex.getMessage());
    	}
    	return sb.toString();
    }
    
    /**
     * Check if the Table already Exists
     * @param tableName
     * @return
     */
    public boolean exitsTable(String tableName) {
        boolean exist = false;
        
        String tableNames = "";

        tableNames = getTables_String();
        if (tableNames.contains(tableName)) {
        	exist = true;
        }

        return exist;
    }
	
	

}

/*
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % D4M: Dynamic Distributed Dimensional Data Model 
 * % MIT Lincoln Laboratory
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % (c) <2010> Massachusetts Institute of Technology
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 */
