/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is used to compare
 * @author cyee
 *
 */
public class CompareUtil {
	Pattern pattern=null;
	String [] array =null;
	/**
	 * 
	 */
	public CompareUtil(String [] array) {
		String regex = RegExpUtil.makeRegex(array);
		pattern = Pattern.compile(regex);
		this.array = array;
	}

	public boolean compareIt(String q) {
		Matcher  match = pattern.matcher(q);
		boolean retval = match.matches();
		
		if(!retval) {
			if(this.array.length == 3 && this.array[1].equals(":") ) {
				retval = (q.compareTo(array[0]) >= 0 && q.compareTo(array[2]) <=0);
			} else if(this.array.length == 2) {
                            
                                 retval = (q.compareTo(array[0]) >= 0 && q.compareTo(array[1]) <=0);
                            
                        } else if(this.array.length == 1) {
                                retval = q.compareTo(array[0]) >= 0;
                        }
		}
		return retval;
	}
	public Pattern getPattern() {
		return this.pattern;
	}
}
