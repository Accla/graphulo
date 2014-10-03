/**
 * 
 */
package edu.mit.ll.d4m.db.cloud.util;

/**
 * @author cyee
 *
 */
public class D4mKeyUtil {

	/*
	 * Convert will extract the row key, column family, column qualifier, and value.
	 * 
	 * key   Key ( org.apache.accumulo.core.data.Key)
	 * value Value ( org.apache.accumulo.core.data.Value)
	 * d4mKey  is the generic object to hold the row, col family, col qualifier, and value
	 */
	public static D4mDataObj convert(Object key, Object value, D4mDataObj d4mKey)  {
	    String rowKey = null; //row key
		String column = null; //ColumnQualifier
		String colFam = null; //Column family
		String val=null;


	 if (key instanceof org.apache.accumulo.core.data.Key) {
			org.apache.accumulo.core.data.Key  theKey = (org.apache.accumulo.core.data.Key)key;
			rowKey = theKey.getRow().toString();
			column = theKey.getColumnQualifier().toString();
			colFam = theKey.getColumnFamily().toString();
			org.apache.accumulo.core.data.Value Val = (org.apache.accumulo.core.data.Value)value;
			val = new String(Val.get());

		}
		d4mKey.setRow(rowKey);
		d4mKey.setColQualifier(column);
		d4mKey.setColFamily(colFam);
		d4mKey.setValue(val);
		return d4mKey;
	}

	public static D4mDataObj convert(Object key, Object value) {
		D4mDataObj d4mkey= new D4mDataObj();
		convert(key,value,d4mkey);
		return d4mkey;
	}
}
