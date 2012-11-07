/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.cb;

import java.util.Iterator;
import java.util.SortedSet;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.TableNotFoundException;
import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.DbInfoIF;

/**
 * CloudbaseInfo retrieve info about cloudbase tables
 * @author cyee
 *
 */
public class CloudbaseInfo implements DbInfoIF {
	private ConnectionProperties connProps = new ConnectionProperties();

	public CloudbaseInfo() {

	}
	/**
	 * 
	 */
	public CloudbaseInfo(ConnectionProperties connProps) {
		this.connProps = connProps;
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.DbInfoIF#getTableList()
	 */
	@Override
	public String getTableList() throws Exception {
	
		return getCloudbaseTableList();
	}
	public String getCloudbaseTableList() throws CBException, CBSecurityException {
		CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);

		SortedSet<?> set = cbConnection.getTableList();
		Iterator<?> it = set.iterator();
		StringBuilder sb = new StringBuilder();
		while (it.hasNext()) {
			String tableName = (String) it.next();
			sb.append(tableName + " ");
		}
		return sb.toString();
	}

	@Override
	public void setConnectionProp(ConnectionProperties connProps) {
		this.connProps = connProps;
		
	}

}
