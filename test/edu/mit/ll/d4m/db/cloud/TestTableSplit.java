package test.edu.mit.ll.d4m.db.cloud;

import java.io.FileNotFoundException;
import java.io.IOException;

import cloudbase.core.client.CBException;
import cloudbase.core.client.CBSecurityException;
import cloudbase.core.client.MutationsRejectedException;
import cloudbase.core.client.TableExistsException;
import cloudbase.core.client.TableNotFoundException;
import edu.mit.ll.d4m.db.cloud.D4mDbInsert;
import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations;

public class TestTableSplit {
    static String host;
    static String instanceName;
    static String tableName;
    static String user="";
    static String password;
    static String startVertexString = "";
    static String endVertexString = "";
    static String weightString = "";
    static int numToPartition=1000;
    static int numOfEntries = 100000;
    static String DELIMITER =",";
    static int LAST_COUNT=1;
    static StringBuffer partitionKeys = new StringBuffer();
    static String REC_PREFIX="X"; //record prefix to make data unique, user can change via command-line
    //    static ArrayList<String> partitionKeys = new ArrayList<String>();
    public TestTableSplit(String host, String instanceName, String tableName, String user, String passwd, String partitionKey,
			  String startVertexString, String endVertexString, String weightString) throws CBException, CBSecurityException, TableExistsException {
	this.host = host;
	this.instanceName=instanceName;
    }

    public static void main(String[] args) throws FileNotFoundException, IOException, CBException, CBSecurityException, TableNotFoundException, MutationsRejectedException, TableExistsException, InterruptedException {

	//args0  host
	//args1  instanceName
	//args2  tableName
	//args3  user
	//args4  password
	//args5  number of records/entries
	//args6  number to partition

	host = args[0];
	instanceName = args[1];
	tableName = args[2];
	user = args[3];
	password = args[4];
	numOfEntries = Integer.valueOf(args[5]);
	numToPartition = Integer.valueOf(args[6]);
	if(args.length == 8)
	    REC_PREFIX=args[7];

	D4mDbInsert d4mdb = new D4mDbInsert(instanceName,host,tableName,user,password);
	D4mDbTableOperations tableOp = new D4mDbTableOperations( instanceName,host,user,password);
	int num_entries = 0;
	int batches =1;
	if(numOfEntries > 500000) {
	    double percentToCut = 0.1;
	    num_entries = (int)((double)numOfEntries * percentToCut);
	    batches = numOfEntries/num_entries;
	    numOfEntries = num_entries;
	}
	System.out.println(" Number of  batches = "+batches);
	System.out.println(" Number of entries per  batch = "+ numOfEntries);
	System.out.println(" Total Number of entries  = "+batches* numOfEntries);
	for (int i=0; i < batches; i++) {
	    generateEntries();
	    
	    d4mdb.doProcessing(startVertexString,endVertexString,weightString);
	    
	    Thread.sleep(10000L);
	}
	//Do table split
	tableOp.splitTable(tableName,partitionKeys.toString());
	//	d4mdb.splitTable(partitionKeys.toString());
    }

    public static void generateEntries() {
		int loops = numOfEntries;
		int capacity = loops;

		StringBuilder sb1 = new StringBuilder(capacity);
		StringBuilder sb2 = new StringBuilder(capacity);
		StringBuilder sb3 = new StringBuilder(capacity);

		System.out.println("Creating test data for " + loops + " entries.");
		int i =0;
		for (i = 0; i < loops ; i++) {
		    String r=REC_PREFIX+LAST_COUNT;
			sb1.append(r + DELIMITER);
			sb2.append(LAST_COUNT + DELIMITER);
			sb3.append(LAST_COUNT + DELIMITER);

			if(LAST_COUNT%numToPartition == 0) {
			    System.out.println("Split at "+r);
			    partitionKeys.append(r);
			    partitionKeys.append(",");
			}
			LAST_COUNT++;
		}
		
		startVertexString = sb1.toString();
		endVertexString = sb2.toString();
		weightString = sb3.toString();

    }

}
