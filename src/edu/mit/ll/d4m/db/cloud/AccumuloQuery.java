/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import edu.mit.ll.d4m.db.cloud.util.D4mDataObj;

/**
 * @author cyee
 *
 */
public class AccumuloQuery implements D4mQueryIF {

	/**
	 * 
	 */
	public AccumuloQuery() {
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#doMatlabQuery(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public void doMatlabQuery(String rows, String cols, String family,
			String authorizations) {
		// TODO Auto-generated method stub

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
		// TODO Auto-generated method stub
		return null;
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
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#setLimit(int)
	 */
	@Override
	public void setLimit(int limit) {
		// TODO Auto-generated method stub

	}

}
