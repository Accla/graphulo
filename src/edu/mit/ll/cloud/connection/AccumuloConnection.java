
/**
 * 
 */
package edu.mit.ll.cloud.connection;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import org.apache.accumulo.core.client.BatchScanner;

/**
 * @author cyee
 *
 */
public class AccumuloConnection {
	private static Logger log = Logger.getLogger(AccumuloConnection.class);

	private ConnectionProperties conn=null;
	private ZooKeeperInstance instance=null;
	private Connector connector= null;
	private Authorizations auth= org.apache.accumulo.core.Constants.NO_AUTHS;
	/**
	 * 
	 */
	public AccumuloConnection(ConnectionProperties conn) {
		this.instance = new ZooKeeperInstance(conn.getInstanceName(), conn.getHost());
		try {
			this.connector = this.instance.getConnector(this.conn.getUser(), this.conn.getPass().getBytes());
			String [] sAuth = conn.getAuthorizations();
			if (sAuth != null) {
				auth = new Authorizations(sAuth);
			}

		} catch (AccumuloException e) {
			log.warn("",e);
		} catch (AccumuloSecurityException e) {
			log.warn(e);
		}
	}

	public void createTable(String tableName) {
		try {
			connector.tableOperations().create(tableName);
		} catch (AccumuloException e) {		
			log.warn(e);
		} catch (AccumuloSecurityException e) {

			log.warn(e);
		} catch (TableExistsException e) {
			log.warn("Table "+ tableName+"  exist.",e);
		}
	}

	// batchwriter
	public BatchWriter createBatchWriter (String table, long maxMemory, long maxLatency,int maxWriteThreads) throws TableNotFoundException {
		return connector.createBatchWriter(table, maxMemory, maxLatency, maxWriteThreads);
	}

	//Scanner
	public Scanner createScanner(String tableName) throws TableNotFoundException {
		return this.connector.createScanner(tableName, this.auth);
	}
	//BatchScanner
	public BatchScanner getBatchScanner(String tableName, int numberOfThreads) throws TableNotFoundException  {
		BatchScanner scanner = connector.createBatchScanner(tableName, this.auth, numberOfThreads);
		return scanner;
	}

	public void deleteTable (String tableName)  {
		try {
			connector.tableOperations().delete(tableName);
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			log.warn(e);
		} catch (AccumuloSecurityException e) {
			log.warn(e);
		} catch (TableNotFoundException e) {
			log.warn(e);
		}
	}


}
