package edu.mit.ll.d4m.db.cloud.util;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.TableExistsException;
import edu.mit.ll.d4m.db.cloud.D4mCbSecurityOperations;

/**
 *  Create table
 * @author cyee
 *
 */
		
public class CreateTable {

	/**
	 * @param args
	 * @throws TableExistsException 
	 * @throws CBSecurityException 
	 * @throws CBException 
	 */
	public static void main(String[] args) throws CBException, CBSecurityException, TableExistsException {
		// TODO Auto-generated method stub
		// rootuser = args[0]
		// password = args[1]
		// host  = args[2]  cb hostname
		// instance  = args[3]

		// table name = args[5]
		if(args.length < 5) {
			System.err.println("USAGE: <root> <root password> <hostname> <instanceName> <username> <tableName> ");
			System.exit(1);
		}
		String rootuser     = args[0];
		String rootpasswd   = args[1];
		String host         = args[2];
		String instanceName = args[3];
		String tableName    = args[4];
		D4mCbSecurityOperations d4mSecOp = 
			new D4mCbSecurityOperations(instanceName,
					host, rootuser, rootpasswd);

		d4mSecOp.createTable(tableName);
	}

}
