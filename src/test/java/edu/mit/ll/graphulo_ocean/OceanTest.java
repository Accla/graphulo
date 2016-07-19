package edu.mit.ll.graphulo_ocean;

import edu.mit.ll.graphulo.DynamicIteratorSetting;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.examples.ExampleUtil;
import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.Connector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

/**
 * Test the ocean genomics pipeline on a small file.
 */
public class OceanTest extends AccumuloTestBase {
  private static final Logger log = LogManager.getLogger(OceanTest.class);

  public static final int kmer = 11;

  @Test
  public void runPipeline() throws Exception {
    String tSampleIDSeqID = "ocsa_TsampleSeq",
        tSampleID = "ocsa_Tsample",
        tSampleDistance = "ocsa_TsampleDis";

    ingestFiles(tSampleIDSeqID);
    sumToSample(tSampleIDSeqID, tSampleID);
    doBrayCurtis(tSampleID, tSampleDistance);
  }

  private void ingestFiles(String tSampleIDSeqID) throws Exception {
    Connector conn = tester.getConnector();
    GraphuloUtil.deleteTables(conn, tSampleIDSeqID);
//    Map<Key,Value> expect = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ),
//        actual = new TreeMap<>(TestUtil.COMPARE_KEY_TO_COLQ);

    CSVIngester ingester = new CSVIngester(conn);
    long numSeqs = ingester.ingestFile(ExampleUtil.getDataFile("S0001_n1000.csv"), tSampleIDSeqID, true);
    numSeqs += ingester.ingestFile(ExampleUtil.getDataFile("S0002_n1000.csv"), tSampleIDSeqID, false);
    log.info("number of sequences ingested: "+numSeqs);

//    Assert.assertEquals(expect, actual);

//    conn.tableOperations().delete(tSampleIDSeqID);
//    conn.tableOperations().delete(tR);
  }


  private void sumToSample(String tSampleIDSeqID, String tSampleID) {
    Connector conn = tester.getConnector();
    GraphuloUtil.deleteTables(conn, tSampleID);
    DynamicIteratorSetting dis = new DynamicIteratorSetting(1, null)
        .append(ValToColApply.iteratorSetting(1))
        .append(KMerColQApply.iteratorSetting(1, kmer));

    Graphulo g = new Graphulo(conn, tester.getPassword());
    long numUniqueKMersPerSample = g.OneTable(tSampleIDSeqID, tSampleID, null, null, -1, null, null, Graphulo.PLUS_ITERATOR_LONG,
        null, null, dis.getIteratorSettingList(), null, null);
    log.info("numUniqueKMersPerSample = "+numUniqueKMersPerSample);
  }

  private void doBrayCurtis(String tSampleID, String tSampleDistance) {
    Connector conn = tester.getConnector();
    GraphuloUtil.deleteTables(conn, tSampleDistance);
    Graphulo g = new Graphulo(conn, tester.getPassword());
    long numSamplePairings = g.cartesianProductBrayCurtis(tSampleID, tSampleDistance,
        CartesianDissimilarityIterator.DistanceType.BRAY_CURTIS);
    log.info("numSamplePairings = "+numSamplePairings);
  }



}