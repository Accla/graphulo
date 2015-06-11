package edu.mit.ll.graphulo.examples;

import edu.mit.ll.graphulo.d4m.D4MTripleFileWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URL;

/**
 * Utility functions used by examples.
 */
public class ExampleUtil {
  private static final Logger log = LogManager.getLogger(ExampleUtil.class);

  static File getDataFile(String name) {
    URL url = Thread.currentThread().getContextClassLoader().getResource("data/"+name);
    if (url == null)
      url = Thread.currentThread().getContextClassLoader().getResource("data/"+name+".gz");
    Assert.assertNotNull(url);
    return new File(url.getPath());
  }

  /** Reads files from src/test/resource/data and inserts into Accumulo using D4M Schema table+transpose+degree. */
  public static void ingestAdjacencySCALE(int SCALE, char version, String baseName, Connector conn) throws FileNotFoundException {
    D4MTripleFileWriter tripleFileWriter = new D4MTripleFileWriter(conn);
    File rowFile = getDataFile(String.valueOf(SCALE)+version+"r.txt");
    File colFile = getDataFile(String.valueOf(SCALE)+version+"c.txt");

    // deleteExistingTables
    long cnt = tripleFileWriter.writeTripleFile_Adjacency(rowFile, colFile, null, ",", baseName, true, false);
    log.info("Wrote "+cnt+" edges to D4M Adjacency tables with base name "+baseName);
  }

  public static void ingestIncidenceSCALE(int SCALE, char version, String baseName, Connector conn) {
//    D4mDbTableOperations d4mtops = new D4mDbTableOperations(conn.getInstance().getInstanceName(), conn.getInstance().getZooKeepers(),
//        conn.whoami(), pass );
    D4MTripleFileWriter tripleFileWriter = new D4MTripleFileWriter(conn);
    File rowFile = getDataFile(String.valueOf(SCALE)+version+"r.txt");
    File colFile = getDataFile(String.valueOf(SCALE)+version+"c.txt");

    // deleteExistingTables
    System.out.println("estimate "+(1 << SCALE)*16);
    long cnt = tripleFileWriter.writeTripleFile_Incidence(rowFile, colFile, null, ",", baseName, true, false, (1 << SCALE) * 16); // upper bound on #edges
    log.info("Wrote "+cnt+" edges to D4M Incidence tables with base name "+baseName);
  }

  public static void ingestIncidenceFromAdjacencySCALE(int SCALE, char version, String baseName, Connector conn) {
//    D4mDbTableOperations d4mtops = new D4mDbTableOperations(conn.getInstance().getInstanceName(), conn.getInstance().getZooKeepers(),
//        conn.whoami(), pass );
    D4MTripleFileWriter tripleFileWriter = new D4MTripleFileWriter(conn);

    // deleteExistingTables
    System.out.println("estimate "+(1 << SCALE)*16);
    long cnt = tripleFileWriter.writeFromAdjacency_Incidence(baseName, true, false, (1 << SCALE) * 16); // upper bound on #edges
    log.info("Wrote "+cnt+" edges to D4M Incidence tables with base name "+baseName);
  }

}
