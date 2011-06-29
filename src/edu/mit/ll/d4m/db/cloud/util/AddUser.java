package edu.mit.ll.d4m.db.cloud.util;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import edu.mit.ll.d4m.db.cloud.D4mCbSecurityOperations;
public class AddUser {

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
		//   args[4] username
		//   args[5] password
		//   args[7 ...N] authorizations  - array of strings
		String rootuser     = args[0];
		String rootpasswd   = args[1];
		String host         = args[2];
		String instanceName = args[3];
		String  username    = args[4];
		String user_passwd  = args[5];
		D4mCbSecurityOperations d4mSecOp = 
			new D4mCbSecurityOperations(instanceName,
					host, rootuser, rootpasswd);

		//Setup the user's authorizations
		String [] authorizations = null;
		if( args.length > 6) {
			authorizations= args[6].split(",");
		}

		System.out.println("USER = "+username+", password = "+user_passwd);
		d4mSecOp.createUser(username, user_passwd, authorizations);


	}

}
