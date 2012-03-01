/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Set;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import cloudbase.core.client.BatchScanner;
import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.Scanner;
import cloudbase.core.client.TableNotFoundException;
import cloudbase.core.data.Key;
import cloudbase.core.data.PartialKey;
import cloudbase.core.data.Range;
import cloudbase.core.data.Value;
import cloudbase.core.iterators.RegExIterator;
import cloudbase.core.iterators.filter.RegExFilter;
import edu.mit.ll.cloud.connection.CloudbaseConnection;
import edu.mit.ll.cloud.connection.ConnectionProperties;
import edu.mit.ll.d4m.db.cloud.util.RegExpUtil;

import edu.mit.ll.d4m.db.cloud.util.D4mDataObj;
import edu.mit.ll.d4m.db.cloud.util.D4mQueryUtil;

/**
 * @author cyee
 *
 */
public class CloudbaseQuery implements D4mQueryIF {
    private static Logger log = Logger.getLogger(CloudbaseQuery.class);
    
    D4mDataObj query=null;
    D4mDataObj results = new D4mDataObj();
    private ConnectionProperties connProps = new ConnectionProperties();
    
    private String tableName=null;
    private int limit =0; //limit number of results returned.
    private int count =0; //
    private Iterator<Entry<Key, Value>> scannerIter =null;
    private QueryResultFilter filter = new QueryResultFilter();

    private Scanner scanner = null;
    private BatchScanner bscanner = null;

    /**
     * 
     */
    public CloudbaseQuery() {
	// TODO Auto-generated constructor stub
    }
    public CloudbaseQuery(String instanceName, String host, String table, String username, String password) {
	this();
	this.tableName = table;
	this.connProps.setHost(host);
	this.connProps.setInstanceName(instanceName);
	this.connProps.setUser(username);
	this.connProps.setPass(password);
    }
    
    /* (non-Javadoc)
     * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#doMatlabQuery(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
	public void doMatlabQuery(String rows, String cols, String family,
				  String authorizations) {
	this.scannerIter = null;
	query = D4mQueryUtil.whatQueryMethod(rows, cols);
	
	if(family != null && family.length() >  0)
	    query.setColFamily(family);

	filter.init(query.getRow(), query.getColQualifier(), query.getMethod());
	this.connProps.setAuthorizations(authorizations.split(","));
	
	switch(query.getMethod()) {
	case GET_ALL_DATA:
	    getAllData(rows,cols,family,authorizations);
	    break;
	case MATLAB_QUERY_ON_COLS:
	    doMatlabQueryOnColumns(rows,cols,family,authorizations);
	    break;
	case MATLAB_RANGE_QUERY_ON_ROWS:
	    doMatlabRangeQueryOnRows(rows,cols,family,authorizations);
	    break;
	case MATLAB_QUERY_ON_ROWS:
	    doMatlabQueryOnRows(rows,cols,family,authorizations);
	    break;
	case SEARCH_BY_ROW_AND_COL:
	    searchByRowAndOnColumns(rows,cols,family,authorizations);
	    break;
	case ASSOC_COLUMN_WITH_ROW:
	    doAssociateColumnWithRow(rows,cols,family,authorizations);
	    break;
	    
	default:
	    break;
	    
	}
	
    }
    
	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#doMatlabQuery(java.lang.String, java.lang.String)
	 */
	@Override
	public void doMatlabQuery(String rows, String cols) {
		// TODO Auto-generated method stub

	}

       public void getAllData(String rows, String cols, String family, String authorizations) {
	   if(this.scannerIter == null ) {
	       try {
	       this.scanner = getScanner();

	       } catch (CBSecurityException e) {

	       }  catch (TableNotFoundException e) {
	       }  catch (CBException e) {
	       }
	       this.scanner.setRange(new Range());
	       this.scanner.fetchColumnFamily(new Text(family));
	       this.scannerIter = scanner.iterator();

	   }
       }
    public void doMatlabQueryOnRows(String rows, String cols, String family, String authorizations) {
	if(scannerIter == null) {
	    //Use BatchScanner
	    HashMap<String, Object> rowMap=null;
	    rowMap = D4mQueryUtil.processParam(rows);
	    String [] rowsArray = (String[])rowMap.get("content");
	    ArrayList<Key> rowKeys = param2keys(rowsArray);
	    HashMap<String,String> sRowMap = D4mQueryUtil.loadRowMap(rows);
	    if(this.bscanner == null) {
		HashSet<Range> ranges = this.loadRanges(rowKeys);
		scanner = getBatchScanner();//cbConnection.getBatchScanner(this.tableName, this.numberOfThreads);
		scanner.setRanges(ranges);
		scanner.fetchColumnFamily(new Text(family));
		
	    }


	}
    }
    
    public void doMatlabRangeQueryOnRows(String rows, String cols, String family, String authorizations) {
	
    }
    
    public void doMatlabQueryOnColumns(String rows, String cols, String family, String authorizations) {
	
    }
    public void searchByRowAndOnColumns(String rows, String cols, String family, String authorizations) {
	
    }
    public void doAssociateColumnWithRow(String rows, String cols, String family, String authorizations) {
	
    }


	private Scanner getScanner() throws CBException, CBSecurityException, TableNotFoundException {
		CloudbaseConnection cbConnection = new CloudbaseConnection(this.connProps);
		if(this.scanner == null)
			this.scanner = cbConnection.getScanner(tableName);
		return scanner;
	}

    /* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#getResults()
	 */
	@Override
	public D4mDataObj getResults() {
		return results;
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#init(java.lang.String, java.lang.String)
	 */
	@Override
	public void init(String rows, String cols) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#next()
	 */
	@Override
	public void next() {
		Entry<Key, Value> entry =null;
		
	    while(scannerIter.hasNext()) {
		if(this.limit == 0 || filter.getCount() < this.limit) {
		    entry = (Entry<Key, Value>) scannerIter.next();

		    String rowKey = entry.getKey().getRow().toString();
		    String column = entry.getKey().getColumnQualifier().toString();
		    //System.out.println(count+"BEFORE_ENTRY="+rowKey+","+column);
		    String value = new String(entry.getValue().get());
		    results.setRow(rowKey);
		    results.setColFamily(entry.getKey().getColumnFamily().toString());
		    results.setColQualifier(column);
		    results.setValue(value);
		    filter.query(results,true);

		} else {
		
		    //		if(this.limit != 0 && filter.getCount() >= this.limit) {
		    break;
		}
	    }

	    results.setRow(filter.getRowResult());
            results.setColQualifier(filter.getColumnResult());
            results.setValue(filter.getValueResult());
	    filter.reset();

	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#hasNext()
	 */
	@Override
	public boolean hasNext() {
	    if(scannerIter != null) {
		return scannerIter.hasNext();
	    }
		return false;
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#setLimit(int)
	 */
	@Override
	public void setLimit(int limit) {
	    this.limit = limit;

	}
	public ArrayList<Key> param2keys(String [] params) {
		ArrayList<Key> keys = new ArrayList<Key> ();
		for(String s : params) {
			if(s.equals(":")) continue;
			Key k = new Key(s);
			keys.add(k);
		}
		return keys;
	}

	public HashSet<Range> loadRanges(HashMap<String, String> queryMap) {
		HashSet<Range> ranges = new HashSet<Range>();
		Iterator<String> it = queryMap.keySet().iterator();
		while (it.hasNext()) {
			String rowId = (String) it.next();
			//System.out.println("==>>ROW_ID="+rowId+"<<++");
			if(rowId != null) {
				Key key = new Key(new Text(rowId));
				//Range range = new Range(key, true, key.followingKey(1), false);
				Range range = new Range(key, true, key.followingKey(PartialKey.ROW), false);


				ranges.add(range);
			}
		}
		return ranges;
	}
	public HashSet<Range> loadRanges(String [] rowsRange) {
	    //		HashSet<Range> ranges = new HashSet<Range>();
		//Iterator<String> it = rangeQuery.iterator();
		//	System.out.println("<<< ROW_ARRAY_LENGTH="+rowsRange.length+" >>>");
	    ArrayList<Key> rowsList = param2keys(rowsRange);
	    //int len = rowsRange.length;
		//for (int i = 0; i < len; i++) {
	    //	for (String rowId :rowsRange ) {
	    //		if(rowId != null && !rowId.equals(":")) {
	    //			Key key = new Key(new Text(rowId));
	    //			rowsList.add(key);
				//Range range = new Range(key, true, key.followingKey(1), false);
				//Range range = new Range(key, true, key.followingKey(PartialKey.ROW), false);
				//ranges.add(range);
	    //		}
	    //		}
		HashSet<Range> ranges = loadRanges(rowsList);
		return ranges;
	}
	public HashSet<Range> loadRanges(ArrayList<Key> rowsRange) {
		HashSet<Range> ranges = new HashSet<Range>();
		for(Key key : rowsRange) {
			Range range = new Range(key, true, key.followingKey(PartialKey.ROW), false);
			ranges.add(range);
		}
		return ranges;

	}

}
