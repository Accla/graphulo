package edu.mit.ll.graphulo.tricount;

import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_BYTES;

public final class TriangleIngestor {
  private static final Logger log = LogManager.getLogger(TriangleIngestor.class);
  private static final FixedIntegerLexicoder LEX = new FixedIntegerLexicoder();

  private final Connector connector;

  @SuppressWarnings("unused") // used in D4M
  public TriangleIngestor(final String instanceName, final String zookeepers, final String username, final String password) throws AccumuloSecurityException, AccumuloException {
    this(new ZooKeeperInstance(instanceName, zookeepers).getConnector(username, new PasswordToken(password)));
  }
  public TriangleIngestor(final Connector connector) {
    this.connector = connector;
  }

  @SuppressWarnings("unused") // used in D4M
  public long ingestDirectory(final String directory,
                              final String tableAdj, final String tableEdge,
                              final boolean reverse, final boolean stringRowCols) {
    // call ingestFile on all pairs of files that end in "XXr.txt", "XXc.txt"
    long count = 0L;

    final File dir = new File(directory);
    if( !dir.isDirectory() )
      throw new IllegalArgumentException(("expected a directry but got "+directory));
    final File[] files = dir.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith("r.txt") || name.endsWith("c.txt");
      }
    });
    Objects.requireNonNull(files, "problem with directory");

    final Map<String,File> prefixMap = new HashMap<>(files.length / 2 + 1);
    for (final File file : files) {
      final String name = file.getName();
      final String prefix = name.substring(0, name.length()-5);
      if( prefixMap.containsKey(prefix) ) {
        final File other = prefixMap.remove(prefix);
        if( name.endsWith("r.txt") )
          count += ingestFile(file, other, tableAdj, tableEdge, reverse, stringRowCols);
        else
          count += ingestFile(other, file, tableAdj, tableEdge, reverse, stringRowCols);
      } else {
        prefixMap.put(prefix, file);
      }
    }
    return count;
  }

  /**
   *
   * @param rowFile File with a big comma-seperated string of ints, aligned with colFile.
   * @param colFile File with a big comma-seperated string of ints, aligned with rowFile.
   * @param tableAdj Adjacency table to ingest into. Can be null.
   * @param tableEdge Incidence tablw to ingest into. Can be null.
   * @param reverse If false, insert adjacency lower triangle and incidence with column labels in ascending order.
   *                If true,  insert adjacency upper triangle and incidence with column labels in descending order.
   * @param stringRowCols If false, encode with 4-bytes. If true, encode with Integer.toString(x).getBytes().
   * @return Number of entries ingested.
   */
  public long ingestFile(final File rowFile, final File colFile,
                         final String tableAdj, final String tableEdge,
                         final boolean reverse, final boolean stringRowCols) {
    if( tableAdj == null && tableEdge == null )
      throw new IllegalArgumentException("need to specify either tableAdj or tableEdge");

    final String delimiter = ",";
    long count = 0, startTime;
     byte[] t1b = new byte[4], t2b = new byte[4], rowcol = new byte[8];

    final BatchWriterConfig bwc = new BatchWriterConfig()
        .setMaxWriteThreads(25).setMaxMemory(1024000).setMaxLatency(100, TimeUnit.MILLISECONDS);
    MultiTableBatchWriter multiBatch = null;

    try (final Scanner rowScanner = new Scanner(rowFile.getName().endsWith(".gz") ? new GZIPInputStream(new FileInputStream(rowFile)) : new FileInputStream(rowFile));
         final Scanner colScanner = new Scanner(colFile.getName().endsWith(".gz") ? new GZIPInputStream(new FileInputStream(colFile)) : new FileInputStream(colFile)))
    {
      rowScanner.useDelimiter(delimiter);
      colScanner.useDelimiter(delimiter);

      GraphuloUtil.createTables(connector, false, tableAdj, tableEdge);

      multiBatch = connector.createMultiTableBatchWriter(bwc);
      final BatchWriter bwAdj, bwEdge;
      if(tableAdj != null) bwAdj = multiBatch.getBatchWriter(tableAdj);
      else bwAdj = null;
      if(tableEdge != null) bwEdge = multiBatch.getBatchWriter(tableEdge);
      else bwEdge = null;
//      final Map<Integer,Integer> map = new HashMap<>(2500000);

      final long origStartTime = startTime = System.currentTimeMillis();

      while (rowScanner.hasNext()) {
        if (!colScanner.hasNext()) {
          throw new IllegalArgumentException("row, col files do not have the same number of elements. " +
              " rowScanner.hasNext()=" + rowScanner.hasNext() +
              " colScanner.hasNext()=" + colScanner.hasNext());
        }
//        final int row, col;
        final byte[] rowb, colb;
        {
          final int t1 = rowScanner.nextInt(), t2 = colScanner.nextInt();
          if( t1 == t2 ) // No Diag.
            continue;
          if( stringRowCols ) {
            t1b = Integer.toString(t1).getBytes(StandardCharsets.UTF_8);
            t2b = Integer.toString(t2).getBytes(StandardCharsets.UTF_8);
          } else {
            LEX.encode(t1, t1b);
            LEX.encode(t2, t2b);
          }
          final int cmp = WritableComparator.compareBytes(t1b, 0, t1b.length, t2b, 0, t2b.length);
          if ((cmp > 0) ^ reverse) { // Lower triangle only, unless reversed.
//            row = t1; col = t2;
            rowb = t1b; colb = t2b;
          } else {
//            row = t2; col = t1;
            rowb = t2b; colb = t1b;
          }
        }
        // consider caching here to remove not insert the same entries twice

        if( tableAdj != null ) {
          final Mutation mutAdj = new Mutation(rowb);
          mutAdj.put(EMPTY_BYTES, colb, EMPTY_BYTES); // empty family, empty value
          bwAdj.addMutation(mutAdj);
          count++;
        }

        if( tableEdge != null ) {
          final Mutation mutEdge = new Mutation(rowb), mutEdge2 = new Mutation(colb);
          bothBytes(colb, rowb, rowcol);
          mutEdge.put(EMPTY_BYTES, rowcol, EMPTY_BYTES);
          mutEdge2.put(EMPTY_BYTES, rowcol, EMPTY_BYTES);
          bwEdge.addMutation(mutEdge);
          bwEdge.addMutation(mutEdge2);
          count += 2;
        }


        if (count % 200000 <= 2) {
          final long stopTime = System.currentTimeMillis();
          if (startTime - stopTime > 1000*60) {
            log.info(String.format("Ingest: %9d cnt, %6d secs, %8d entries/sec on %s, %s%n", count, (stopTime - origStartTime)/1000,
                Math.round(count / ((stopTime - origStartTime)/1000.0)), tableAdj, tableEdge));
            startTime = stopTime;
          }
        }
      }

    } catch(IOException | AccumuloSecurityException | AccumuloException e){
      log.warn("", e);
      throw new RuntimeException(e);
    } catch(TableNotFoundException e){
      log.error("crazy", e);
      throw new RuntimeException(e);
    } finally {
      if (multiBatch != null)
        try {
          multiBatch.close();
        } catch (MutationsRejectedException e) {
          log.error("mutations rejected on close", e);
        }
    }
    return count;
  }

  private void bothBytes(final byte[] a, final byte[] b, final byte[] r) {
    System.arraycopy(a, 0, r, 0, 4);
    System.arraycopy(b, 0, r, 4, 4);
  }

}
