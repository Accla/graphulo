package edu.mit.ll.graphulo.examples;

import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.IMultiplyOp;
import edu.mit.ll.graphulo.mult.LongMultiply;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Example demonstrating
 * (1) ingest the adjacency matrix representation of a graph into the D4M Schema tables ex10A, ex10AT, ex10ADeg;
 * do the same for a second table B;
 * (2) multiply A*B and store the result in Accumulo table ex10C;
 * (3) count the number of entries in ex10C.
 */
public class TableMultExample extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(TableMultExample.class);

  /**
   * Corresponds to saved files in the distribution.
   */
  public static final int SCALE = 10;

  @Test
  public void exampleTableMult() throws FileNotFoundException, TableNotFoundException {
    String Atable = "ex" + SCALE + "A";     // Adjacency table A
    String ATtable = "ex" + SCALE + "AT";   // Adjacency table A Transpose (row and column qualifier switched)
    String Btable = "ex" + SCALE + "B";     // Adjacency table B
    String Ctable = "ex" + SCALE + "C";     // Adjacency table C to contain the operation result

    // In your code, you would connect to an Accumulo instance by writing something similar to:
//    ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance("instance").withZkHosts("localhost:2181").withZkTimeout(5000);
//    Instance instance = new ZooKeeperInstance(cc);
//    Connector c = instance.getConnector("root", new PasswordToken("secret"));
    // Here, we connect to the Accumulo instance given by TEST_CONFIG.java.
    // You can change this by passing the option -DTEST_CONFIG=local or -DTEST_CONFIG=txe1 or similar.
    Connector conn = tester.getConnector();

    // Insert data from the file test/resources/data/10Ar.txt and 10Ac.txt into Accumulo.
    // Deletes tables if they already exist.
    ExampleUtil.ingestSCALE(SCALE, 'A', Atable, conn);
    // Insert second table from 10Br.txt and 10Bc.txt
    ExampleUtil.ingestSCALE(SCALE, 'B', Btable, conn);

    // Create Graphulo executor. Supply the password for your Accumulo user account.
    Graphulo graphulo = new Graphulo(conn, tester.getPassword());

    // Plus operation. Satisfies requirement that 0 is additive identity.
    int sumPriority = 6;
    IteratorSetting plusOp = new IteratorSetting(sumPriority, SummingCombiner.class);
    // Options for plus operator: encode/decode with a string representation; act on all columns of Ctable.
    LongCombiner.setEncodingType(plusOp, LongCombiner.Type.STRING);
    Combiner.setCombineAllColumns(plusOp, true);
    // Note: this is the same as Graphulo.DEFAULT_PLUS_ITERATOR

    // Other options to TableMult
    String CTtable = null;                // Don't write the table transpose.
    Class<? extends IMultiplyOp> multOp = LongMultiply.class; // Multiply operation.
    //                                    multOp satisfies requirement that 0 is multiplicative annihilator.
    Collection<Range> rowFilter = null;   // No row subsetting; run on whole tables.
    String colFilterAT = null;            // No column subsetting for ATtable; run on the whole table.
    String colFilterB = null;             // No column subsetting for  Btable; run on the whole table.
    int numEntriesCheckpoint = -1;        // Don't monitor TableMult progress.
    boolean trace = false;                // Don't record performance times at the server.

    // Matrix multiply A*B.  Uses +.* algebra.
    // This call blocks until the multiply completes,
    // i.e., until all partial products are sent to Ctable.
    graphulo.TableMult(ATtable, Btable, Ctable, CTtable, multOp, plusOp,
        rowFilter, colFilterAT, colFilterB, numEntriesCheckpoint, trace);

    // Result is in Ctable. Do whatever you like with it.
    BatchScanner bs = conn.createBatchScanner(Ctable, Authorizations.EMPTY, 2);
    bs.setRanges(Collections.singleton(new Range()));   // Scan whole table.
    int cnt = 0;
    for (Map.Entry<Key, Value> entry : bs) {
      cnt++;
    }
    bs.close();
    log.info("# of entries in output table " + Ctable + ": " + cnt);

  }

}
