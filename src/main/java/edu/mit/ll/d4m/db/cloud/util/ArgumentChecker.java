/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.util;

/**
 * @author cyee
 *
 */
public class ArgumentChecker {
	  private static final String NULL_ARG_MSG = "argument was null";

	private ArgumentChecker() {
	}

	public static void notNull(final Object arg1) {
	    if (arg1 == null)
	      throw new IllegalArgumentException(NULL_ARG_MSG + ":Is null- arg1? ");
	  }
	  
	  public static void notNull(final Object arg1, final Object arg2) {
	    if (arg1 == null || arg2 == null)
	      throw new IllegalArgumentException(NULL_ARG_MSG + ":Is null- arg1? " + (arg1 == null) + " arg2? " + (arg2 == null));
	  }
	  
	  public static void notNull(final Object arg1, final Object arg2, final Object arg3) {
	    if (arg1 == null || arg2 == null || arg3 == null)
	      throw new IllegalArgumentException(NULL_ARG_MSG + ":Is null- arg1? " + (arg1 == null) + " arg2? " + (arg2 == null) + " arg3? " + (arg3 == null));
	  }
	  
	  public static void notNull(final Object arg1, final Object arg2, final Object arg3, final Object arg4) {
	    if (arg1 == null || arg2 == null || arg3 == null || arg4 == null)
	      throw new IllegalArgumentException(NULL_ARG_MSG + ":Is null- arg1? " + (arg1 == null) + " arg2? " + (arg2 == null) + " arg3? " + (arg3 == null)
	          + " arg4? " + (arg4 == null));
	  }
	  
	  public static void notNull(final Object[] args) {
	    if (args == null)
	      throw new IllegalArgumentException(NULL_ARG_MSG + ":arg array is null");
	    
	    for (int i = 0; i < args.length; i++)
	      if (args[i] == null)
	        throw new IllegalArgumentException(NULL_ARG_MSG + ":arg" + i + " is null");
	  }
	  
	  public static void strictlyPositive(final int i) {
	    if (i <= 0)
	      throw new IllegalArgumentException("integer should be > 0, was " + i);
	  }
}
