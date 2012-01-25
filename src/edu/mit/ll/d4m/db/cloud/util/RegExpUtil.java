/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.util;

/**
 * RegExpUtil :  Regular expression utility class
 * 
 * @author cyee
 *
 */
public class RegExpUtil {

	/*
	 *  str -  String array - eg   a,:,b
	 *   This array follows the Matlab-like syntax
	 *    str[0] = a
	 *    str[1] = :
	 *    str[2] = b
	 *  
	 */
	public static String makeRegex(String [] str) {
		String s=null;
		if(str.length != 3)
			return s;
		
		String s0 = str[0];
		String s2 = str[2];
		StringBuffer sb = new StringBuffer();
		s="^("+str[0]+")|^("+str[2]+").*|";
		sb.append(s);

		sb.append("^[");
		sb.append(s0.substring(0, 1));
		sb.append("-");
		sb.append(s2.substring(0, 1));
		sb.append("]");
		
		if(s0.length() > 1 && s2.length() > 1) {
			sb.append("[");
			sb.append(s0.substring(1, 2));
			sb.append("-");
			sb.append(s2.substring(1, 2));
			sb.append("]");
		}
		sb.append(".*");
		
//		s="|^("+str[0]+")|^("+str[2]+").*";
//		sb.append(s);
		s = sb.toString();
		return s;
	}
}
