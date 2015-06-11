package edu.mit.ll.graphulo.examples;

import edu.mit.ll.graphulo.util.AccumuloTestBase;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.FileNotFoundException;

/**
 * Created by dhutchis on 6/11/15.
 */
public class EdgeBFSExample extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(AdjBFSExample.class);

  /** Corresponds to saved files in the test/java/resources/data folder. */
  public static final int SCALE = 10;

  public static final int numSteps = 3;

  @Test
  public void exampleEdgeBFS() throws FileNotFoundException, TableNotFoundException {
    String Atable = "ex" + SCALE + "A";                 // Adjacency table A.
    String Rtable = "ex" + SCALE + "Astep" + numSteps;   // Result of BFS is summed into Rtable.
    String RTtable = null;                              // Don't write transpose of BFS.
    String ADegtable = "ex" + SCALE + "ADeg";           // Adjacency table A containing out-degrees.
    String degColumn = "out";                           // Name of column qualifier under which out-degrees appear in ADegtable.
    boolean degInColQ = false;                          // Degree is stored in the Value, not the Column Qualifier.
    int minDegree = 20;                                 // Bounding minimum degree: only include nodes with degree 20 or higher.
    int maxDegree = Integer.MAX_VALUE;                  // Unbounded maximum degree.  This + the minimum degree make a High-pass Filter.
    String v0 = "1,25,33,";                             // Starting nodes: start from node 1 (the supernode) and a few others.
    boolean trace = false;                              // Disable debug printing.

    // In your code, you would connect to an Accumulo instance by writing something similar to:
//    ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance("instance").withZkHosts("localhost:2181").withZkTimeout(5000);
//    Instance instance = new ZooKeeperInstance(cc);
//    Connector c = instance.getConnector("root", new PasswordToken("secret"));
    // Here, we connect to the Accumulo instance given by TEST_CONFIG.java.
    // You can change this by passing the option -DTEST_CONFIG=local or -DTEST_CONFIG=txe1 or similar.
    Connector conn = tester.getConnector();

    // Insert data from the file test/resources/data/10Ar.txt and 10Ac.txt into Accumulo.
    // Deletes tables if they already exist.
    ExampleUtil.ingestIncidenceSCALE(SCALE, 'A', Atable, conn);

//    // Create Graphulo executor. Supply the password for your Accumulo user account.
//    Graphulo graphulo = new Graphulo(conn, tester.getPassword());
//
//    // Configure options for sum operator.
//    // We choose to use Accumulo's SummingCombiner as the plus operation.
//    // This iterator decodes values as longs and sums them using long-type addition.
//
//    // Plus operation. Satisfies requirement that 0 is additive identity.
//    int sumPriority = 6;
//    IteratorSetting plusOp = new IteratorSetting(sumPriority, SummingCombiner.class);
//    // Options for plus operator: encode/decode with a string representation; act on all columns of Ctable.
//    LongCombiner.setEncodingType(plusOp, LongCombiner.Type.STRING);
//    Combiner.setCombineAllColumns(plusOp, true);
//    // Note: this is the same as Graphulo.DEFAULT_PLUS_ITERATOR
//
//    // Adjacency Table Breadth First Search.
//    // This call blocks until the BFS completes.
//    graphulo.AdjBFS(Atable, v0, numSteps, Rtable, RTtable,
//        ADegtable, degColumn, degInColQ, minDegree, maxDegree, plusOp, trace);
//
//    // Result is in output table. Do whatever you like with it.
//    BatchScanner bs = conn.createBatchScanner(Rtable, Authorizations.EMPTY, 2);
//    bs.setRanges(Collections.singleton(new Range()));   // Scan whole table.
//    int cnt = 0;
//    for (Map.Entry<Key, Value> entry : bs) {
//      cnt++;
//    }
//    bs.close();
//    log.info("# of entries in output table '" + Rtable + ": " + cnt);
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  /*  Variations of above example:

      Change the minimum and maximum degrees, the starting nodes and the plus operation.
      Setting plusOp to null means that entries sent to Rtable overwrite existing entries
      instead of summing.

      Increase the SCALE parameter to 12, 14 or 16 to run on larger graphs.

  */
  ////////////////////////////////////////////////////////////////////////////////////////////////

}
