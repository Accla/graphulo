/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.util;

import org.apache.log4j.Logger;

import edu.mit.ll.d4m.db.cloud.QueryResultFilter;

/**
 * RegExpUtil :  Regular expression utility class
 * 
 * @author cyee
 *
 */
public class RegExpUtil {
	private static Logger log = Logger.getLogger(RegExpUtil.class);

	/*
	 *  str -  String array - eg   a,:,b
	 *   This array follows the Matlab-like syntax
	 *    str[0] = a
	 *    str[1] = :
	 *    str[2] = b
	 *  
	 */
	public static String makeRegex(String [] str) {
		String s="";
		if(str.length == 1) {
			s=str[0];
		} else if (str.length == 2) {
			if(!str[0].equals(":")) {
				s=str[0] +"|";
			} else {
				s=".*";
				return s;
			}
			if(!str[1].equals(":")) {
				s = s+str[1];
			} else {
				s = s+".*";
			}

		}
		else if(str.length == 3) {
			StringBuffer sb = new StringBuffer();
			if( str[1].equals(":")) {
				String s0 = str[0];
				String s2 = str[2];
				log.debug("[0] = "+s0 + " ,[2]="+s2);

				s="^"+str[0]+"|^"+str[2]+".*";
				//s="("+str[0]+")|("+str[2]+".*)|";
				sb.append(s);

                                if(s0.length() >= 1) {
				   sb.append("|").append("^[");
				   sb.append(s0.substring(0,1)).append("");
                                     if(s2.length() >= 1) {
                                         sb.append("-").append(s2.substring(0,1));
                                     } 
                                     sb.append("]");
                                     if(s0.length() > 1) {
                                        int len0 = s0.length();
                                        sb.append("(").append(s0.substring(1)).append(")");
                                     }
                                     if(s2.length() > 1) {
                                        sb.append("(").append(s2.substring(1)).append(")");
                                     }
                                }
				//sb.append("-");
				//sb.append("(").append(s2).append(")].");
				//if(s0.length() > 1)
				//	sb.append("*(").append(s0.substring(1)).append(")");
				//if(s0.length() > 1 && s2.length() > 1) {
				//	sb.append("|");//.append(s2.substring(1)).append(")");
				//}  
				//if(s2.length() > 1) {
			//		sb.append("*(").append(s2.substring(1)).append(")");
			//	}
				//				if(s0.length() > 1 && s2.length() > 1) {
				//					sb.append("[");
				//					sb.append(s0.substring(1, 2));
				//					sb.append("-");
				//					sb.append(s2.substring(1, 2));
				//					sb.append("]");
				//				}
			} else {
                              sb.append(str[0]).append("|");
                              sb.append(str[1]).append("|");
                              sb.append(str[2]);
                        }
			//sb.append(".*");

			//		s="|^("+str[0]+")|^("+str[2]+").*";
			//		sb.append(s);
			s = sb.toString();
		}

		if(log.isDebugEnabled()) {
			log.debug("LENGTH OF STR ARRAY = "+str.length+ ",   "+s);
			//System.out.println("LENGTH OF STR ARRAY = "+str.length+ ",   "+s);
		}

		return s;
	}

	public static String regexMapper(String regex) {

		String charStr = regex.replace("*", "");
		String reg = "^" + charStr + "*|^" + charStr + ".";
		return reg;
	}

}
