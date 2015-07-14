package edu.mit.ll.graphulo.examples;

import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Map;

/**
 * Example demonstrating
 * (1) ingest the adjacency matrix representation of a graph into the D4M Schema tables ex10A, ex10AT, ex10ADeg;
 * (2) create a new Accumulo table ex10J holding the Jaccard coefficients in the upper triangle of a subset of ex10A;
 * (3) count the number of entries in ex10Astep3.
 */
public class JaccardExample extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(JaccardExample.class);

  /** Corresponds to saved files in the test/java/resources/data folder. */
  public static final int SCALE = 10;

  @Test
  public void exampleJaccard() throws FileNotFoundException, TableNotFoundException, AccumuloSecurityException, AccumuloException {
    String Atable = "ex" + SCALE + "A";                 // Adjacency table A.
    String Rtable = "ex" + SCALE + "J";                 // Table to write Jaccard coefficients.
    String ADegtable = "ex" + SCALE + "ADeg";           // Adjacency table A containing out-degrees.
    boolean trace = false;                              // Disable debug printing.

    // In your code, you would connect to an Accumulo instance by writing something similar to:
//    ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance("instance").withZkHosts("localhost:2181").withZkTimeout(5000);
//    Instance instance = new ZooKeeperInstance(cc);
//    Connector c = instance.getConnector("root", new PasswordToken("secret"));
    // Here, we connect to the Accumulo instance given by TEST_CONFIG.java.
    // You can change this by passing the option -DTEST_CONFIG=local or -DTEST_CONFIG=txe1 or similar.
    Connector conn = tester.getConnector();

    // Delete result table if it exists, so that we don't sum in previous runs with our results.
    if (conn.tableOperations().exists(Rtable))
      conn.tableOperations().delete(Rtable);

    // Insert data from the file test/resources/data/10Ar.txt and 10Ac.txt into Accumulo.
    // Deletes tables if they already exist.
    ExampleUtil.ingestAdjacencySCALE(SCALE, 'A', Atable, conn);

    // Create Graphulo executor. Supply the password for your Accumulo user account.
    Graphulo graphulo = new Graphulo(conn, tester.getPassword());

    // This call blocks until the BFS completes.
    long npp = graphulo.Jaccard(Atable, ADegtable, Rtable, trace);log.info("Number of Jaccard coefficients in result table: " + npp);
    log.info("Number of partial products sent to result table: " + npp);

    // Result is in output table. Do whatever you like with it.
    BatchScanner bs = conn.createBatchScanner(Rtable, Authorizations.EMPTY, 2);
    bs.setRanges(Collections.singleton(new Range()));   // Scan whole table.
    int nnzJaccard = 0;
    for (Map.Entry<Key, Value> entry : bs) {
      nnzJaccard++;
    }
    bs.close();
    log.info("Number of Jaccard coefficients in result table: " + nnzJaccard);
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  /*  Variations of above example:

  1)  Change the minimum and maximum degrees, the starting nodes and the plus operation.
      Setting plusOp to null means that entries sent to Rtable overwrite existing entries
      instead of summing.

  2)  Increase the SCALE parameter to 12, 14 or 16 to run on larger graphs.

  3)  Set ADegtable to null to instruct Graphulo to filter degrees on the fly using an iterator.

  4)  Set Rtable and RTtable both to null to obtain the nodes reachable
      in exactly numSteps as a return value from the BFS call,
      without writing the subgraph traversed at each step to result tables.

  */
  ////////////////////////////////////////////////////////////////////////////////////////////////


}
