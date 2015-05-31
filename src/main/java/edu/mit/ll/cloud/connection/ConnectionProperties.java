package edu.mit.ll.cloud.connection;

public class ConnectionProperties {

	private String host;
	private String user;
	private String pass;
	private String instanceName;
	private String [] authorizations=null;
	public static int MAX_NUM_THREADS=25;
    private int maxNumThreads=MAX_NUM_THREADS;  //50
    private int sessionTimeOut=100000; //millisec

    public ConnectionProperties() {}
    
	public ConnectionProperties(String host, String user, String pass,
			String instanceName, String[] authorizations) {
		this.host = host;
		this.user = user;
		this.pass = pass;
		this.instanceName = instanceName;
		this.authorizations = authorizations;
	}

	public String[] getAuthorizations() {
		return authorizations;
	}

	public void setAuthorizations(String[] authorizations) {
		this.authorizations = authorizations;
	}

	/**
	 * @return the host
	 */
	public String getHost() {
		return host;
	}

	/**
	 * @param host
	 *            the host to set
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 * @param user
	 *            the user to set
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * @return the pass
	 */
	public String getPass() {
		return pass;
	}

	/**
	 * @param pass
	 *            the pass to set
	 */
	public void setPass(String pass) {
		this.pass = pass;
	}

	/**
	 * @return the instanceName
	 */
	public String getInstanceName() {
		return instanceName;
	}

	/**
	 * @param instanceName
	 *            the instanceName to set
	 */
	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
	}

    public void setMaxNumThreads( int num) {
	this.maxNumThreads=num;
    }
   public int getMaxNumThreads( ) {
	return this.maxNumThreads;
    }
   public String toString() {
	   String s = "INSTANCE_NAME="+this.instanceName+", HOST="+this.host+", USER="+this.user;
	   return s;
   }

public int getSessionTimeOut() {
	return sessionTimeOut;
}

public void setSessionTimeOut(int sessionTimeOut) {
	this.sessionTimeOut = sessionTimeOut;
}
 }
/*
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % D4M: Dynamic Distributed Dimensional Data Model 
 * % MIT Lincoln Laboratory
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 
 * % (c) <2010> Massachusetts Institute of Technology
 * %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
 */
