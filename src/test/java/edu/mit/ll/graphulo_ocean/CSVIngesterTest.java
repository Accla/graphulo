package edu.mit.ll.graphulo_ocean;

import edu.mit.ll.graphulo.examples.ExampleUtil;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * Ingest two sample snippets.
 */
public class CSVIngesterTest extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(CSVIngesterTest.class);

  @Test
  public void ingestFile() throws Exception {
    Connector conn = tester.getConnector();
    final String tA, tR;
//    {
//      String[] names = getUniqueNames(2);
//      tA = names[0];
//      tR = names[1];
//    }
    tA = "ocsa_Traw";
//    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
//        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    CSVIngester ingester = new CSVIngester(conn);
    ingester.ingestFile(ExampleUtil.getDataFile("S0001_n1000.csv"), tA, true);
    ingester.ingestFile(ExampleUtil.getDataFile("S0002_n1000.csv"), tA, false);



//    Assert.assertEquals(expect, actual);

//    conn.tableOperations().delete(tA);
//    conn.tableOperations().delete(tR);
  }

}