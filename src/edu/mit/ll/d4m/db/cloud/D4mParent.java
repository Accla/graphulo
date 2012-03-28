package edu.mit.ll.d4m.db.cloud;

import org.apache.log4j.Logger;

public abstract class D4mParent {
	private static Logger log = Logger.getLogger(D4mParent.class);

    public D4mParent() {}

    public void setCloudType(String cloudType) {
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
