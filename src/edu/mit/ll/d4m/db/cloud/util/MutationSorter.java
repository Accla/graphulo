/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.util;

import java.util.HashMap;

/**
 * MutationSorter sorts the mutation according to row id
 * 
 * @author CHV8091
 *
 */
public class MutationSorter {

	private HashMap <String, Object> map=null;
	/**
	 * 
	 */
	public MutationSorter() {
		map = new HashMap<String,Object> ();
	}

	public void add(String rowkey,Object mutation) {
		map.put(rowkey, mutation);
	}
	
	public Object get(String rowkey) {
		Object mut = null;
		mut = map.get(rowkey);
		return mut;
	}
	
	public boolean hasMutation(String rowkey) {
		return map.containsKey(rowkey);
	}
}
