package edu.mit.ll.d4m.db.cloud;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

public abstract class D4mParent {
	private static Logger log = null; //Logger.getLogger(D4mParent.class);

	private static boolean isReady=false;
	
	public D4mParent() {
		init();
	}
	
	private void init() {
		if(!isReady){
			try {
				DOMConfigurator.configure(this.getClass().getClassLoader().getResource("log4j.xml"));
				isReady=true;
			} catch (Exception e) {
				System.err.println(e);
			}
		}

	}

	public void setCloudType(String cloudType) {
		if(log == null)
			log = Logger.getLogger(D4mParent.class);
		D4mConfig d4mConf = D4mConfig.getInstance();
		log.debug("CLOUD TYPE = "+cloudType);
		d4mConf.setCloudType(cloudType);
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
