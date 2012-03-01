/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import edu.mit.ll.d4m.db.cloud.util.D4mDataObj;

/**
 * Common query interface for Cloudbase and Accumulo
 * @author cyee
 *
 */
public interface D4mQueryIF {

	//exec  execute the query
	public void doMatlabQuery(String rows, String cols, String family, String authorizations);
	public void doMatlabQuery(String rows, String cols);
	
	// get the results
	public D4mDataObj getResults();
	// init initialize the query
	//Do I need an init method?
	public void init(String rows, String cols);
	
	public void next();
	public boolean hasNext();
	public void setLimit(int limit);
}
