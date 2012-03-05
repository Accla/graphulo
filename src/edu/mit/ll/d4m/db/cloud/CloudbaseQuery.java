/**
 * 
 */
package edu.mit.ll.d4m.db.cloud;

import java.io.IOException;
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
public class CloudbaseQuery extends D4mQueryBase {
	private static Logger log = Logger.getLogger(CloudbaseQuery.class);

	//    D4mDataObj query=null;
	//    D4mDataObj results = new D4mDataObj();
	//    private ConnectionProperties connProps = new ConnectionProperties();
	//    
	//    private String tableName=null;
	//    private int limit =0; //limit number of results returned.
	//    private int count =0; //
	private Iterator<Entry<Key, Value>> scannerIter =null;
	//    private QueryResultFilter filter = new QueryResultFilter();

	private Scanner scanner = null;
	private BatchScanner bscanner = null;

	/**
	 * 
	 */
	public CloudbaseQuery() {
		super();
	}
	public CloudbaseQuery(String instanceName, String host, String table, String username, String password) {
		super(instanceName,host,table,username,password);
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#doMatlabQuery(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	/*
	@Override
	public void doMatlabQuery(String rows, String cols, String family,
			String authorizations) {
		clear();

	super.doMatlabQuery(rows, cols, family, authorizations);
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
*/
	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#doMatlabQuery(java.lang.String, java.lang.String)
	 */
	@Override
	public void doMatlabQuery(String rows, String cols) {
		// TODO Auto-generated method stub

	}
	@Override
	public void getAllData(String rows, String cols, String family, String authorizations) {
		log.debug(" %%%% getAllData %%%%");
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
	@Override
	public void doMatlabQueryOnRows(String rows, String cols, String family, String authorizations) {
		log.debug(" %%%% doMatlabQueryOnRows %%%%");
		if(scannerIter == null) {
			//Use BatchScanner
			HashMap<String, Object> rowMap=null;
			rowMap = D4mQueryUtil.processParam(rows);
			String [] rowsArray = (String[])rowMap.get("content");
			ArrayList<Key> rowKeys = param2keys(rowsArray);
			HashMap<String,String> sRowMap = D4mQueryUtil.loadRowMap(rows);
			if(this.bscanner == null) {
				HashSet<Range> ranges = this.loadRanges(rowKeys);
				try {
					bscanner = getBatchScanner();
				} catch (CBException e) {
					// TODO Auto-generated catch block
					log.warn(e);
				} catch (CBSecurityException e) {
					// TODO Auto-generated catch block
					log.warn(e);
				} catch (TableNotFoundException e) {
					// TODO Auto-generated catch block
					log.warn(e);
				}//cbConnection.getBatchScanner(this.tableName, this.numberOfThreads);
				bscanner.setRanges(ranges);
				bscanner.fetchColumnFamily(new Text(family));
				this.scannerIter = this.bscanner.iterator();
			}


		}
	}
	@Override
	public void doMatlabRangeQueryOnRows(String rows, String cols, String family, String authorizations) {
		log.debug("%%%% doMatlabRangeQueryOnRows %%%%");
		HashMap<String, Object> rowMap = null;
		String regexParams=null;
		if (this.scannerIter == null) {
			rowMap =D4mQueryUtil.processParam(rows);
			String[] rowArray = (String[]) rowMap.get("content");
			Range range = null;
			if (D4mQueryUtil.getRangeQueryType(rowArray).equals(D4mDbQuery.KEY_RANGE)) {
				Key startKey = new Key(new Text(rowArray[0]));
				Key endKey = new Key(new Text(rowArray[2]));
				range = new Range(startKey, true, endKey, true);

			} else if (D4mQueryUtil.getRangeQueryType(rowArray).equals(D4mDbQuery.POSITIVE_INFINITY_RANGE)) {
				Key startKey = new Key(new Text(rowArray[0]));
				range = new Range(startKey, true, null, true);


			} else if (D4mQueryUtil.getRangeQueryType(rowArray).equals(D4mDbQuery.NEGATIVE_INFINITY_RANGE)) {
				Key endKey = new Key(new Text(rowArray[2]));
				//Range range = new Range(null, true, endKey.followingKey(1), false);
				range = new Range(null, true, endKey.followingKey(PartialKey.ROW), false);

			}
			else if (D4mQueryUtil.getRangeQueryType(rowArray).equals(D4mDbQuery.REGEX_RANGE)) {
				regexParams = RegExpUtil.regexMapper(rowArray[0]);
				range = new Range();			

			}

			try {
				this.scanner = getScanner();
			} catch (CBException e) {
				// TODO Auto-generated catch block
				log.warn(e);
			} catch (CBSecurityException e) {
				// TODO Auto-generated catch block
				log.warn(e);
			} catch (TableNotFoundException e) {
				// TODO Auto-generated catch block
				log.warn(e);
			}
			this.scanner.setRange(range);
			if(regexParams != null)
				scanner.setRowRegex(regexParams);	
			scanner.fetchColumnFamily(new Text(family));
			this.scannerIter = this.scanner.iterator();

		}
	}
	@Override
	public void doMatlabQueryOnColumns(String rows, String cols, String family, String authorizations) {
		log.debug(" <<<< doMatlabQueryOnColumns >>>> ");
		if(this.scannerIter == null) {
			HashMap<?, ?> rowMap = D4mQueryUtil.loadColumnMap(cols);
			HashMap<String,Object> objColMap = D4mQueryUtil.processParam(cols);
			String [] colArray = (String []) objColMap.get("content");
			try {
				scanner = getScanner();
			} catch (CBException e) {
				// TODO Auto-generated catch block
				log.warn(e);
			} catch (CBSecurityException e) {
				// TODO Auto-generated catch block
				log.warn(e);
			} catch (TableNotFoundException e) {
				// TODO Auto-generated catch block
				log.warn(e);
			}
			Range range =  new Range();
			scanner.setRange(range);
			scanner.fetchColumnFamily(new Text(family));
			this.scannerIter = scanner.iterator();

		}

	}
	@Override
	public void searchByRowAndOnColumns(String rows, String cols, String family, String authorizations) {
		log.debug(" <<<< searchByRowAndOnColumns >>>> ");

		HashMap<String, Object> rowMap = null;
		rowMap = D4mQueryUtil.processParam(rows);
		HashMap<String, Object> columnMap = null;
		columnMap =D4mQueryUtil.processParam(cols);
		HashSet<Range> ranges = null;
		Range range = null;
		String[] rowArray = (String[]) rowMap.get("content");
		String[] columnArray = (String[]) columnMap.get("content");
		int length=rowArray.length;
		String rowkey1 = null;
		String rowkey2 = null;
		if(length == 1) {
			rowkey1 = rowArray[0];
			rowkey2 = rowArray[0];
			range = new Range(rowkey1,true,rowkey2, true);
		} else if(length == 2 & !rowArray[(length-1)].equals(":")) { 
			ranges = loadRanges(rowArray);

		}  else if(length == 3) {
			if(rowArray[1].equals(":")) {
				rowkey1 = rowArray[0];
				rowkey2 = rowArray[length-1];
			} else if( rowArray[(length-1)].equals(":")) {
				rowkey1 = rowArray[0];
				rowkey2 = rowArray[1];
			}
			range = new Range(rowkey1,true,rowkey2, true);

		}
		String colRegex = RegExpUtil.makeRegex(columnArray);
		Pattern colPattern = Pattern.compile(colRegex);
		log.debug("COLUMN PATTERN =   "+colPattern.pattern() + "   ===>>>   "+ colRegex);

		if(range != null) {

			try {
				this.scanner = getScanner();
			} catch (CBException e1) {
				e1.printStackTrace();
			} catch (CBSecurityException e1) {
				e1.printStackTrace();
			} catch (TableNotFoundException e1) {
				e1.printStackTrace();
			}
			this.scanner.setRange(range);
			if(columnArray.length == 3 && columnArray[1].equals(":")) {
				String regexName="D4mRegEx";
				try {
					this.scanner.setScanIterators(1, RegExIterator.class.getName(), regexName);
					this.scanner.setScanIteratorOption(regexName, RegExFilter.COLQ_REGEX, colRegex);

				} catch (IOException e) {
					// TODO Auto-generated catch block
					log.warn(e);
				}

			}
			scanner.fetchColumnFamily(new Text(family));
			this.scannerIter = scanner.iterator();
		} else if (ranges != null) {
		//	Pattern colPattern = Pattern.compile(colRegex);
			log.debug("COLUMN PATTERN =   "+colPattern.pattern());
			try {
				bscanner = getBatchScanner();
				bscanner.setRanges(ranges);
				String regexName="D4mRegEx";
				bscanner.setScanIterators(1, RegExIterator.class.getName(), regexName);
				bscanner.setScanIteratorOption(regexName, RegExFilter.COLQ_REGEX, colPattern.pattern());

			} catch (CBException e) {
				log.warn(e);
			} catch (CBSecurityException e) {
				log.warn(e);
			} catch (TableNotFoundException e) {
				log.warn(e);
			} catch (IOException e) {
				log.warn(e);
			}
			bscanner.fetchColumnFamily(new Text(family));
			this.scannerIter = bscanner.iterator();
		}



	}
	@Override
	public void doAssociateColumnWithRow(String rows, String cols, String family, String authorizations) {
		log.debug(" <<<< doAssociateColumnWithRow >>>> ");

		HashMap<String, String> rowMap=null;
		rowMap = D4mQueryUtil.assocColumnWithRow(rows, cols);
		HashMap<String,Object>objRowMap = D4mQueryUtil.processParam(rows);
		String [] rowsArray = (String[])objRowMap.get("content");
		ArrayList<Key>rowKeys = param2keys(rowsArray);
		HashSet<Range> ranges = this.loadRanges(rowKeys);
		try {
			bscanner =  getBatchScanner();
		} catch (CBException e) {
			// TODO Auto-generated catch block
			log.warn(e);
		} catch (CBSecurityException e) {
			// TODO Auto-generated catch block
			log.warn(e);
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			log.warn(e);
		} 
		bscanner.setRanges(ranges);
		bscanner.fetchColumnFamily(new Text(family));
		this.scannerIter = this.bscanner.iterator();
	}


	private Scanner getScanner() throws CBException, CBSecurityException, TableNotFoundException {
		CloudbaseConnection cbConnection = new CloudbaseConnection(super.connProps);
		if(this.scanner == null)
			this.scanner = cbConnection.getScanner(tableName);
		return scanner;
	}
	private BatchScanner getBatchScanner() throws CBException, CBSecurityException, TableNotFoundException {
		CloudbaseConnection cbConnection = new CloudbaseConnection(super.connProps);
		if(this.bscanner == null)
			this.bscanner = cbConnection.getBatchScanner(super.tableName, super.numberOfThreads);
		return this.bscanner;
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#getResults()
	 */
	@Override
	public D4mDataObj getResults() {
		return super.getResults();
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
				String value = new String(entry.getValue().get());
				log.info("ENTRY="+rowKey+","+column+","+value);

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
		super.limit = limit;

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

	public void clear() {
		this.scannerIter = null;

		if(this.bscanner != null) {
			this.bscanner.close();
		}
		if(this.scanner != null) {
			this.scanner.clearColumns();
			this.scanner.clearScanIterators();
		}
	}
}
