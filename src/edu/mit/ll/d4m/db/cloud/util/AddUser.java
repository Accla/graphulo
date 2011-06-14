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
		// command option = args[4], ie createuser, grantsystempermission, granttablepermission
		
		
		// createuser
		//   args[5] username
		//   args[6] password
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
		
		String [] authorizations = new String [args.length - 6];
		int j = 0;
		for(int i = 6; i < args.length; i++) {
			authorizations[j] = args[i];
			System.out.println("Add authorization  = "+authorizations[j]);
			j++;
		}
		
		System.out.println("USER = "+username+", password = "+user_passwd);
		d4mSecOp.createUser(username, user_passwd, authorizations);

		
	}

}
