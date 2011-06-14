/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.util;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.security.SystemPermission;
import edu.mit.ll.d4m.db.cloud.D4mCbSecurityOperations;

/**
 * This class is used to grant system permissions to a user.
 * This implies that you are root in order to grant use 
 * privileges.
 * 
 * @author cyee
 *
 */
public class GrantSystemPermission {

	/**
	 * @param args
	 * @throws CBSecurityException 
	 * @throws CBException 
	 */
	public static void main(String[] args) throws CBException, CBSecurityException {
		// rootuser = args[0]
		//password = args[1]
		//host  = args[2]  cb hostname
		//instance  = args[3]
		// username args[4]
		
		//  args[5]   permission - CREATE_TABLE, DROP_TABLE,ALTER_TABLE

		String rootuser     = args[0];
		String rootpasswd   = args[1];
		String host         = args[2];
		String instanceName = args[3];
		String  username    = args[4];
		String permission = args[5];
		
		
		SystemPermission sysPerm= SystemPermission.valueOf(permission);
		
		System.out.println("User = "+username+", System permission="+sysPerm.toString());
		
		D4mCbSecurityOperations d4mSecOp = 
			new D4mCbSecurityOperations(instanceName,
					host, rootuser, rootpasswd);
		//grantSystemPermission(String user, String permission)
		d4mSecOp.grantSystemPermission(username, permission);

	}

}
