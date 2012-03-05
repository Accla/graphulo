/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.log4j.Logger;


import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.util.D4mDataObj;
import edu.mit.ll.d4m.db.cloud.util.D4mQueryUtil;

/**
 * 
 * Base class for D4M querying
 * @author CHV8091
 *
 */
public abstract class D4mQueryBase implements D4mQueryIF {
	private static Logger log = Logger.getLogger(D4mQueryBase.class);

	protected D4mDataObj query=null;
	protected D4mDataObj results = new D4mDataObj();
	protected ConnectionProperties connProps = new ConnectionProperties();

	protected String tableName=null;
	protected int limit =0; //limit number of results returned.
	protected int count =0; //
	protected int numberOfThreads = 50;
	protected QueryResultFilter filter = new QueryResultFilter();


	public D4mQueryBase() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	public D4mQueryBase(String instanceName, String host, String table, String username, String password) {
		this.tableName = table;
		this.connProps.setHost(host);
		this.connProps.setInstanceName(instanceName);
		this.connProps.setUser(username);
		this.connProps.setPass(password);

	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#doMatlabQuery(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void doMatlabQuery(String rows, String cols, String family,
			String authorizations) {
		clear();
		query = D4mQueryUtil.whatQueryMethod(rows, cols);
		//	query = D4mQueryUtil.whatQueryMethod(rows, cols);

		if(family != null && family.length() >  0)
			query.setColFamily(family);

		filter.init(query.getRow(), query.getColQualifier(), query.getMethod());
		if(authorizations != null && authorizations.length() > 0)
			this.connProps.setAuthorizations(authorizations.split(","));

		switch(query.getMethod()) {
		case GET_ALL_DATA:
			getAllData(rows,cols,family,authorizations);
			break;
		case MATLAB_QUERY_ON_COLS:
			doMatlabQueryOnColumns(rows,cols,family,authorizations);
			break;
		case MATLAB_RANGE_QUERY_ON_ROWS:
			doMatlabRangeQueryOnRows(rows,cols,family,authorizations);
			break;
		case MATLAB_QUERY_ON_ROWS:
			doMatlabQueryOnRows(rows,cols,family,authorizations);
			break;
		case SEARCH_BY_ROW_AND_COL:
			searchByRowAndOnColumns(rows,cols,family,authorizations);
			break;
		case ASSOC_COLUMN_WITH_ROW:
			doAssociateColumnWithRow(rows,cols,family,authorizations);
			break;

		default:
			break;

		}


	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#doMatlabQuery(java.lang.String, java.lang.String)
	 */
	@Override
	public void doMatlabQuery(String rows, String cols) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#getResults()
	 */
	@Override
	public D4mDataObj getResults() {
		if(log.isInfoEnabled() || log.isDebugEnabled()) {
			results.setRowList(filter.getRowList());
		}
		return this.results;
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#init(java.lang.String, java.lang.String)
	 */
	@Override
	public void init(String rows, String cols) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#next()
	 */
	@Override
	public void next() {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return false;
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#setLimit(int)
	 */
	@Override
	public void setLimit(int limit) {
		this.limit = limit;

	}

	abstract public void getAllData(String rows, String cols, String family, String authorizations);
	abstract public void doMatlabQueryOnRows(String rows, String cols, String family, String authorizations) ;
	abstract public void doMatlabRangeQueryOnRows(String rows, String cols, String family, String authorizations);
	abstract public void doMatlabQueryOnColumns(String rows, String cols, String family, String authorizations) ;
	abstract public void searchByRowAndOnColumns(String rows, String cols, String family, String authorizations);
	abstract public void doAssociateColumnWithRow(String rows, String cols, String family, String authorizations);
	abstract public void clear();
	public int getCount() {
		return this.count;
	}

	public ConnectionProperties getConnProps() {
		return connProps;
	}

	public void setConnProps(ConnectionProperties connProps) {
		this.connProps = connProps;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

}
