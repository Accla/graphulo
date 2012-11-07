/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.cb;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.TableExistsException;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.security.TablePermission;
/**
 * This class is used to grant table permissions to a user.
 * If the table does not exist then it would create it.
 * 
 * @author cyee
 *
 */
public class CloudbaseGrantTablePermissions {

	/**
	 * @param args
	 * @throws CBSecurityException 
	 * @throws CBException 
	 * @throws TableExistsException 
	 * @throws TableNotFoundException 
	 */
	public static void main(String[] args) throws CBException, CBSecurityException, TableNotFoundException, TableExistsException {
		// rootuser = args[0]
		// password = args[1]
		// host  = args[2]  cb hostname
		// instance  = args[3]
		// username args[4]
		// table name = args[5]
		// table permission =  args[6]   permission - READ, WRITE, BULK_IMPORT
		if(args.length < 7) {
			System.err.println("USAGE: <root> <root password> <hostname> <instanceName> <username> <tableName> <table permission>");
			System.exit(1);
		}
		String rootuser     = args[0];
		String rootpasswd   = args[1];
		String host         = args[2];
		String instanceName = args[3];
		String  username    = args[4];
		String tableName    = args[5];
		String permission   = args[6];

		TablePermission tabPermiss = TablePermission.valueOf(permission);
		System.out.println("USER="+username+", permission = "+tabPermiss.toString());
		CloudbaseD4mSecurityOperations d4mSecOp = 
			new CloudbaseD4mSecurityOperations(instanceName,
					host, rootuser, rootpasswd);
		d4mSecOp.grantTablePermission(username, tableName, tabPermiss);
		
	}

}
