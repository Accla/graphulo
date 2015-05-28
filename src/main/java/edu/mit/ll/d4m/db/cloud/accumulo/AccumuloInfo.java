/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.accumulo;

import java.util.Iterator;
import java.util.SortedSet;

import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.DbInfoIF;

/**
 * @author cyee
 *
 */
public class AccumuloInfo implements DbInfoIF {
	private ConnectionProperties connProps = null;

	public AccumuloInfo() {
	}

	/**
	 * 
	 */
	public AccumuloInfo(ConnectionProperties connProps) {
		this.connProps = connProps;
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.DbInfoIF#getTableList()
	 */
	@Override
	public String getTableList() throws Exception {
	
		return getAccumuloTableList();
	}

	public String getAccumuloTableList() throws Exception {
		AccumuloConnection connection = new AccumuloConnection(this.connProps);

		SortedSet<String> set = connection.getTableList();
		Iterator<String> it = set.iterator();
		StringBuilder sb = new StringBuilder();
		while (it.hasNext()) {
			String tableName = (String) it.next();
			sb.append(tableName + " ");
		}
		return sb.toString();
	}

	@Override
	public void setConnectionProp(ConnectionProperties connProps) {
		this.connProps= connProps;
		
	}

}
