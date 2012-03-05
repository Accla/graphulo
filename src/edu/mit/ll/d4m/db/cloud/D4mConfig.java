/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

/**
 * D4M configuration
 * @author CHV8091
 *
 */
public class D4mConfig {

	private  String cloudType = "BigTableLike";
	public static String PROP_D4M_CLOUD_TYPE="d4m.cloud.type";
	
	private static D4mConfig instance= null;
	public D4mConfig() {
		
	}
	public static D4mConfig getInstance() {
		if(instance == null)
		{
			instance = new D4mConfig();
		}
		return instance;
	}
	public String getCloudType() {
		return cloudType;
	}
	public void setCloudType(String cloudType) {
		this.cloudType = cloudType;
	}
}
