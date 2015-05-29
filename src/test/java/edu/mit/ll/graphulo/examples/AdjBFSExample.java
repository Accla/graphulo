package edu.mit.ll.graphulo.examples;

import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
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
import java.util.Collections;
import java.util.Map;

/**
 * Example demonstrating
 * (1) ingest the adjacency matrix representation of a graph into the D4M Schema tables ex10A, ex10AT, ex10ADeg;
 * (2) create a new Accumulo table ex10step3 with the union sum of three BFS steps from node 1;
 * (3) count the number of entries in ex10step3.
 */
public class AdjBFSExample extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(AdjBFSExample.class);

  /**
   * Corresponds to saved files in the distribution.
   */
  public static final int SCALE = 10;

  public static final int numSteps = 3;

  @Test
  public void exampleAdjBFS() throws FileNotFoundException, TableNotFoundException {
    final String inputTable = "ex" + SCALE + "A";
    final String outputTable = "ex" + SCALE + "step" + numSteps;
    /** Start from node 1 (the supernode) and a few others. */
    final String v0 = "1,25,33,";

    // In your code, you would connect to an Accumulo instance by writing somehting similar to:
//    ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance("instance").withZkHosts("localhost:2181").withZkTimeout(5000);
//    Instance instance = new ZooKeeperInstance(cc);
//    Connector c = instance.getConnector("root", new PasswordToken("secret"));
    // Here, we connect to the Accumulo instance given by TEST_CONFIG.java.
    // You can change this by passing the option -DTEST_CONFIG=local or -DTEST_CONFIG=txe1 or similar.
    Connector conn = tester.getConnector();

    // Insert data from the file test/resources/data/10r.txt and 10c.txt into Accumulo.
    // Deletes tables if they already exist.
    ExampleUtil.ingestSCALE(SCALE, inputTable, conn);

    // Create Graphulo executor. Supply your password.
    Graphulo graphulo = new Graphulo(conn, tester.getPassword());

    // Configure options for sum operator.
    // We choose to use Accumulo's SummingCombiner as the plus operation.
    // This iterator decodes values as longs and sums them using long-type addition.
    int sumPriority = 6;
    IteratorSetting sumSetting = new IteratorSetting(sumPriority, SummingCombiner.class);
    LongCombiner.setEncodingType(sumSetting, LongCombiner.Type.STRING);
    Combiner.setCombineAllColumns(sumSetting, true);

    // Adjacency Table Breadth First Search.
    // This call blocks until the BFS completes.
    graphulo.AdjBFS(inputTable, v0, numSteps, outputTable,
        null,                             // Don't write the transpose of the result table.
        inputTable + "Deg", "deg", false, // Information on degree table.
        20, Integer.MAX_VALUE,            // Filter out nodes with degrees less than 20. (High-pass Filter)
        sumSetting, false);               // Use our plus operation on the result table.

    // Result is in output table. Do whatever you like with it.
    BatchScanner bs = conn.createBatchScanner(outputTable, Authorizations.EMPTY, 2);
    bs.setRanges(Collections.singleton(new Range()));   // Scan whole table.
    int cnt = 0;
    for (Map.Entry<Key, Value> entry : bs) {
      cnt++;
    }
    bs.close();
    log.info("# of entries in output table " + outputTable + ": " + cnt);
  }

}
