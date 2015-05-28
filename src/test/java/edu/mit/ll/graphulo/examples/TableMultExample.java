package edu.mit.ll.graphulo.examples;

import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.mult.LongMultiply;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
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
 * (2) multiply transpose(A) * A and store the result in Accumulo table ex10C;
 * (3) count the number of entries in ex10C.
 */
public class TableMultExample extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(TableMultExample.class);

  /** Corresponds to saved files in the distribution. */
  public static final int SCALE=10;

  @Test
  public void exampleTableMult() throws FileNotFoundException, TableNotFoundException {
    final String inputTable = "ex"+SCALE+"A";
    final String outputTable = "ex"+SCALE+"C";

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
    IteratorSetting sumSetting = new IteratorSetting(7, LongCombiner.class);
    LongCombiner.setEncodingType(sumSetting, LongCombiner.Type.STRING);
    LongCombiner.setCombineAllColumns(sumSetting, true);

    // Matrix multiply transpose(A)*A.
    // This call blocks until the multiply completes,
    // i.e., until all partial products are sent to outputTable.
    graphulo.TableMult(inputTable,inputTable, outputTable,
        LongMultiply.class, sumSetting,
        null, null, null);  // no row or column subsetting; run on the whole table

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
