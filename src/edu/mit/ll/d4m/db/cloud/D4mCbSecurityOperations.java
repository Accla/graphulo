/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.admin.SecurityOperations;
import cloudbase.core.security.SystemPermission;
import cloudbase.core.security.TablePermission;
import edu.mit.ll.cloud.connection.CloudbaseConnection;
import edu.mit.ll.cloud.connection.ConnectionProperties;

import org.apache.log4j.Logger;

/**
 * @author cyee
 *
 */
public class D4mCbSecurityOperations {
	private static Logger log = Logger.getLogger(D4mCbSecurityOperations.class);
	private String user="";
	private String password="";
	private CloudbaseConnection connection=null;
	private ConnectionProperties connProps= new ConnectionProperties();
	
	public D4mCbSecurityOperations(String instanceName, String hostname, String rootuser, String password) {
		this.user = rootuser;
		this.password = password;
		this.connProps.setHost(hostname);
		this.connProps.setInstanceName(instanceName);
		this.connProps.setUser(this.user);
		this.connProps.setPass(password);
		try {
			this.connection = new CloudbaseConnection(connProps);
		} catch (CBException e) {
			// TODO Auto-generated catch block
			log.warn(e);
		} catch (CBSecurityException e) {
			// TODO Auto-generated catch block
			log.warn(e);
		}
	}
	
	/*
	 * Root can grant system permission to a user.
	 *  params
	 *       param[0]  user to grant permission to 
	 *       param[1]  permission
	 */
	public void grantsystempermission(ArrayList<String> params) throws CBException, CBSecurityException {
		grantSystemPermission(params.get(0), params.get(1));
	}
	
	/*
	 *  user     user to grant permission to
	 *  permission  CREATE_TABLE, DROP_TABLE, ALTER_TABLE, CREATE_USER,DROP_USER,ALTER_USER,SYSTEM,GRANT
	 */
	public void grantSystemPermission(String user, String permission) throws CBException, CBSecurityException {
		this.connection = new CloudbaseConnection(connProps);
		SystemPermission sysPerm = null;
		for(SystemPermission sp : SystemPermission.values()) {
			if(sp.toString().equals(permission)) {
				sysPerm = sp;
				break;
			}
		}
		this.connection.grantSystemPermission(user,  sysPerm);
	}
	/*
	 * Create user    Root can create a new user.
	 *  params
	 *      param[0]  user name
	 *      param[1]  password of user
	 *      param[2 ... N]  authorizations of this user
	 */
	public void createuser(ArrayList<String> params) throws CBException, CBSecurityException {
		String user = params.get(0);
		String passwd = params.get(1);
		String [] authorizations = new String[params.size()-2];
		int j = 0;
		for(int i = 2;i < params.size();i++) {
			authorizations[j] = params.get(i);
			j++;
		}
		createUser(user,passwd, authorizations);
	}
	public void createUser(String user, String password, String [] authorizations) throws CBException, CBSecurityException  {
		this.connection.addUser(user,password, authorizations);
	}
	
	public void granttablepermission(ArrayList<String> params) throws CBException, CBSecurityException  {
		String user = params.get(0);
		String tablename = params.get(1);
		String permission = params.get(2);
		TablePermission tablePermission = null;
		for(TablePermission tp: TablePermission.values()) {
			if(tp.toString().equals(permission)) {
				tablePermission = tp;
				break;
			}
		}
		grantTablePermission(user,tablename, tablePermission);
	}
	
	public void grantTablePermission(String username, String tableName, TablePermission permission) throws CBException, CBSecurityException  {
		this.connection.grantTablePermission(username,tableName,permission);
	}
	public void process(String ...strings ) {
		String cmd = strings[0];
		log.debug("CMD = "+cmd);
		
		int len = strings.length -1;
		Object [] params = new Object[1];
		
		//Add parameters to  an arraylist since it is easier to use  reflection to 
		// to invoke the appropriate method.
		ArrayList<String> tmpList = new ArrayList<String>();
		int j=0;
		for(int i = 1; i < strings.length; i++) {
			log.info(" param["+j+"]="+strings[i]);
			tmpList.add(strings[i]);
			j++;
		}
		params[0] = tmpList;
		log.debug("PARAMS length = "+params.length);
		Method [] methods = this.getClass().getDeclaredMethods();
		Method meth =null;
		for(int i = 0; i < methods.length; i++) {
			if(methods[i].getName().equals(cmd)) {
				meth = methods[i];
				break;
			}
		}
		try {
			if(meth != null) {
				log.debug("Execute method = "+ meth.getName());
			meth.invoke(this, params);
			}
		} catch (IllegalArgumentException e) {
			log.warn(e);
		} catch (IllegalAccessException e) {
			log.warn(e);
		} catch (InvocationTargetException e) {
			log.warn(e+"  - "+meth.getName());
		}
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// rootuser = args[0]
		//password = args[1]
		//host  = args[2]
		//instance  = args[3]
		// command option = args[4], ie createuser, grantsystempermission, granttablepermission
		
		// grantsystempermission
		//  args[5]  username
		//  args[6]   permission - CREATE_TABLE, DROP_TABLE,ALTER_TABLE
		
		// createuser
		//   args[5] username
		//   args[6] password
		//   args[7 ...N] authorizations  - array of strings
		
		// granttablepermission
		//    args[5]  username
		//    args[6]   tablename
		//    args[7]   table permission -  READ, WRITE, BULK_IMPORT
		String rootuser=args[0];
		String password = args[1];
		String host = args[2];
		String instanceName = args[3];

		String [] params = new String [args.length - 4];
		int j=0;
		for(int i= 4; i < args.length; i++) {
			params[j] = args[i];
			j++;
		}
		
		D4mCbSecurityOperations d4mSecOp = 
			new D4mCbSecurityOperations(instanceName,
					host, rootuser, password);
		
		d4mSecOp.process(params);
	}

}
/*
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% D4M: Dynamic Distributed Dimensional Data Model
% MIT Lincoln Laboratory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% (c) <2010> Massachusetts Institute of Technology
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
*/

