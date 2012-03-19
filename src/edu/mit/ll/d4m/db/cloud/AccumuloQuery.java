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
import java.util.regex.Pattern;
import java.util.Set;

import edu.mit.ll.cloud.connection.AccumuloConnection;
import edu.mit.ll.d4m.db.cloud.util.D4mDataObj;
import edu.mit.ll.d4m.db.cloud.util.D4mQueryUtil;
import edu.mit.ll.d4m.db.cloud.util.RegExpUtil;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.RegExIterator;
import org.apache.accumulo.core.iterators.filter.RegExFilter;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;


/**
 * @author cyee
 *
 */
public class AccumuloQuery extends D4mQueryBase {
	private static Logger log = Logger.getLogger(AccumuloQuery.class);

	private Iterator<Entry<Key,Value>> scannerIter =null;
	private BatchScanner bscanner = null;
	private Scanner scanner       = null;

	/**
	 * 
	 */
	public AccumuloQuery() {
		super();
	}
	public AccumuloQuery(String instanceName, String host, String table, String username, String password) {
		super(instanceName,host,table,username,password);
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#doMatlabQuery(java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	//	@Override
	//	public void doMatlabQuery(String rows, String cols, String family,
	//			String authorizations) {
	//		super.doMatlabQuery(rows, cols, family, authorizations);
	//
	//
	//
	//	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#doMatlabQuery(java.lang.String, java.lang.String)
	 */
//	@Override
//	public void doMatlabQuery(String rows, String cols) {
		// TODO Auto-generated method stub

//	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#getResults()
	 */
	@Override
	public D4mDataObj getResults() {
		// TODO Auto-generated method stub
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
		filter.reset();
		if(this.scannerIter == null) return;
		while(this.scannerIter.hasNext()) {
			count++;
			if(super.limit == 0 || filter.getCount() < super.limit) {
				entry =  scannerIter.next();
				if(entry != null) {
					Key key = entry.getKey();
					if(key != null) {
						Text tRow = entry.getKey().getRow();
						if(tRow != null) {
							log.debug(count+" --> I am in the loop , limit = "+super.limit);

							String rowKey = tRow.toString();
							String column = entry.getKey().getColumnQualifier().toString();
							String family = entry.getKey().getColumnFamily().toString();
							if(D4mConfig.DEBUG) {
								System.out.println(count+"--BEFORE_ENTRY="+rowKey+","+column);
							}
							String value = new String(entry.getValue().get());
							results.setRow(rowKey);
							results.setColFamily(entry.getKey().getColumnFamily().toString());
							results.setColQualifier(column.replace(family, ""));
							results.setValue(value);
							filter.query(results,true);
						}
					}else {
						break;
					}
				} else {
					break;
				}
			} else {
				break;
			}
		}

		results.setRow(filter.getRowResult());
		results.setColQualifier(filter.getColumnResult());
		results.setValue(filter.getValueResult());
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#hasNext()
	 */
	@Override
	public boolean hasNext() {
		if(this.scannerIter != null)
			return this.scannerIter.hasNext();
		return false;
	}

	/* (non-Javadoc)
	 * @see edu.mit.ll.d4m.db.cloud.D4mQueryIF#setLimit(int)
	 */
	@Override
	public void setLimit(int limit) {
		super.setLimit(limit);
	}

	public void clear() {
		if(this.bscanner != null) {
			this.bscanner.close();
			this.bscanner = null;
		}
		if(this.scanner != null) {
			this.scanner.clearColumns();
			this.scanner.clearScanIterators();
			this.scanner = null;
		}
		this.scannerIter = null;
	}


	@Override
	public void getAllData(String rows, String cols, String family,
			String authorizations) {
		if(this.scannerIter == null ) {

			try {
				this.scanner = getScanner();
				this.scanner.setRange(new Range());
				this.scanner.fetchColumnFamily(new Text(family));
				this.scannerIter = scanner.iterator();

			} catch (TableNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				//	clear();
			}

		}


	}
	private Scanner getScanner() throws TableNotFoundException {
		if(this.scanner == null) {
			AccumuloConnection connect = new AccumuloConnection(super.connProps);
			this.scanner= connect.createScanner(tableName);
		}
		return this.scanner;
	}
	private BatchScanner getBatchScanner() throws TableNotFoundException {
		if(this.bscanner == null) {
			AccumuloConnection connect = new AccumuloConnection(super.connProps);
			this.bscanner = connect.getBatchScanner(tableName,super.numberOfThreads);
		}
		return this.bscanner;
	}
	@Override
	public void doMatlabQueryOnRows(String rows, String cols, String family,
			String authorizations) {
		if(scannerIter == null) {
			//Use BatchScanner
			HashMap<String, Object> rowMap=null;
			rowMap = D4mQueryUtil.processParam(rows);

			String [] rowsArray = (String[])rowMap.get("content");
			ArrayList<Key> rowKeys = param2keys(rowsArray);
			HashMap<String,String> sRowMap = D4mQueryUtil.loadRowMap(rows);
			//if(this.bscanner == null) {
			//HashSet<Range> ranges = this.loadRanges(rowKeys);
			Range range = this.loadRange(rowsArray);
			try {
				//	bscanner = getBatchScanner();
				this.scanner = getScanner();
			} catch (TableNotFoundException e) {
				// TODO Auto-generated catch block
				log.warn(e);
			}//cbConnection.getBatchScanner(this.tableName, this.numberOfThreads);
			//bscanner.setRanges(ranges);
			//bscanner.fetchColumnFamily(new Text(family));
			scanner.setRange(range);
			scanner.fetchColumnFamily(new Text(family));
			this.scannerIter = this.scanner.iterator();//this.bscanner.iterator();
			//}


		}

	}
	private Range loadRange(String [] array) {
		Range range=null;
		if(array.length == 1) {
			range = new Range(array[0]);
		} else if(array.length == 2) {
			range = new Range(array[0], array[1]);
		}
		else {
			range = new Range();
		}
		return range;
	}
	public HashSet<Range> loadRanges(String [] rowsRange) {
		ArrayList<Key> rowsList = param2keys(rowsRange);
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

	public ArrayList<Key> param2keys(String [] params) {
		ArrayList<Key> keys = new ArrayList<Key> ();
		for(String s : params) {
			if(s.equals(":")) continue;
			Key k = new Key(s);
			keys.add(k);
		}
		return keys;
	}

	@Override
	public void doMatlabRangeQueryOnRows(String rows, String cols,
			String family, String authorizations) {
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

			} catch (TableNotFoundException e) {
				// TODO Auto-generated catch block
				log.warn(e);
			}
			this.scanner.setRange(range);
			//if(regexParams != null)
			//scanner.setRowRegex(regexParams);	
			scanner.fetchColumnFamily(new Text(family));
			this.scannerIter = this.scanner.iterator();

		}

	}
	@Override
	public void doMatlabQueryOnColumns(String rows, String cols, String family,
			String authorizations) {
		log.debug(" <<<< doMatlabQueryOnColumns >>>> ");
		if(this.scannerIter == null) {
			//			HashMap<?, ?> rowMap = D4mQueryUtil.loadColumnMap(cols);
			//			HashMap<String,Object> objColMap = D4mQueryUtil.processParam(cols);
			//			String [] colArray = (String []) objColMap.get("content");
			try {
				scanner = getScanner();
			}  catch (TableNotFoundException e) {
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
	public void searchByRowAndOnColumns(String rows, String cols,
			String family, String authorizations) {
		if(this.scannerIter == null) {
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
			if(range != null) {

				try {
					this.scanner = getScanner();
				}  catch (TableNotFoundException e1) {
					e1.printStackTrace();
				}
				this.scanner.setRange(range);
				if(columnArray.length == 3 && columnArray[1].equals(":")) {
					String regexName="D4mRegEx";
					try {
						this.scanner.setScanIterators(1, RegExIterator.class.getName(), regexName);
						this.scanner.setScanIteratorOption(regexName, RegExFilter.COLQ_REGEX, colRegex);
					} catch (IOException e) {
						log.warn(e);
					}


				}
				scanner.fetchColumnFamily(new Text(family));
				this.scannerIter = scanner.iterator();
			} else if (ranges != null) {
				Pattern colPattern = Pattern.compile(colRegex);
				try {

					if(log.isDebugEnabled()) {
						log.debug("Setup BatchScanner ");
					}
					bscanner = getBatchScanner();
					bscanner.setRanges(ranges);
					
					//if(columnArray.length == 3 && columnArray[1].equals(":")) {
						String regexName="D4mRegEx";
						//bscanner.setColumnQualifierRegex(colRegex);
						String iterClsName=  RegExIterator.class.getName();
						//org.apache.accumulo.core.iterators.WholeRowIterator.class.getName();
						bscanner.setScanIterators(1, iterClsName, regexName);
						bscanner.setScanIteratorOption(regexName, RegExFilter.COLQ_REGEX, colPattern.pattern());
					//}
					
				} catch (TableNotFoundException e) {
					log.warn(e);
				} 
				catch (IOException e) {
					log.warn(e);
				}
				bscanner.fetchColumnFamily(new Text(family));
				this.scannerIter = bscanner.iterator();
			}
		}

	}
	@Override
	public void doAssociateColumnWithRow(String rows, String cols,
			String family, String authorizations) {
		HashMap<String, String> rowMap=null;
		rowMap = D4mQueryUtil.assocColumnWithRow(rows, cols);
		HashMap<String,Object>objRowMap = D4mQueryUtil.processParam(rows);
		String [] rowsArray = (String[])objRowMap.get("content");
		ArrayList<Key>rowKeys = param2keys(rowsArray);
		HashSet<Range> ranges = this.loadRanges(rowKeys);
		try {
			bscanner =  getBatchScanner();
		} catch (TableNotFoundException e) {
			log.warn(e);
		} 
		bscanner.setRanges(ranges);
		bscanner.fetchColumnFamily(new Text(family));
		this.scannerIter = this.bscanner.iterator();

	}
}
