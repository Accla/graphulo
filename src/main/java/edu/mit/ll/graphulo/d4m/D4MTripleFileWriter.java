package edu.mit.ll.graphulo.d4m;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

/**
 * Write row, column and (optionally) value files to a table.
 */
public class D4MTripleFileWriter {
  private static final Logger log = LogManager.getLogger(D4MTripleFileWriter.class);

  private Connector connector;

  public D4MTripleFileWriter(Connector connector) {
    this.connector = connector;
  }

  /**
   * Writes triples from component files to a main table, transpose table and degree table.
   *
   * @param valFile Optional value file. Uses "1" if not given.
   * @param delimiter Delimiter that separates items.
   * @param baseName Name of tables is the base name plus "", "T", "Deg"
   * @param deleteExistingTables Delete tables if present.
   * @param trackTime Log the rate of ingest or not.
   * @return Number of triples written.
   */
  public long writeTripleFile(File rowFile, File colFile, File valFile,
                              String delimiter, String baseName,
                              boolean deleteExistingTables, boolean trackTime) {
    long count = 0, startTime, origStartTime;
    Text row = new Text(), col = new Text(), valText = null;
    Value val = D4MTableWriter.VALONE;

    Scanner valScanner = null;
    try (Scanner rowScanner = new Scanner( rowFile.getName().endsWith(".gz") ? new GZIPInputStream(new FileInputStream(rowFile)) : new FileInputStream(rowFile) );
         Scanner colScanner = new Scanner( colFile.getName().endsWith(".gz") ? new GZIPInputStream(new FileInputStream(colFile)) : new FileInputStream(colFile) )) {
      rowScanner.useDelimiter(delimiter);
      colScanner.useDelimiter(delimiter);
      if (valFile != null) {
        valScanner = new Scanner(valFile);
        valScanner.useDelimiter(delimiter);
        valText = new Text();
      }

      D4MTableWriter.D4MTableConfig config = new D4MTableWriter.D4MTableConfig();
      config.baseName = baseName;
      config.useTable = config.useTableT = true;
      config.sumTable = config.sumTableT = true;
      config.useTableDeg = true;
      config.useTableDegT = true;
      config.useTableField = config.useTableFieldT = false;
      config.useSameDegTable = true;
      config.colDeg = new Text("out");
      config.colDegT = new Text("in");
      config.connector = connector;
      config.deleteExistingTables = deleteExistingTables;

      origStartTime = startTime = System.currentTimeMillis();
      try (D4MTableWriter tw = new D4MTableWriter(config)) {
        while (rowScanner.hasNext()) {
          if (!colScanner.hasNext() || (valScanner != null && !valScanner.hasNext())) {
            throw new IllegalArgumentException("row, col and val files do not have the same number of elements. " +
                " rowScanner.hasNext()=" + rowScanner.hasNext() +
                " colScanner.hasNext()=" + colScanner.hasNext() +
                (valScanner == null ? "" : " valScanner.hasNext()=" + valScanner.hasNext()));
          }
          row.set(rowScanner.next());
          col.set(colScanner.next());
          if (valFile != null) {
            valText.set(valScanner.next());
            val.set(valText.getBytes());
          }
          tw.ingestRow(row, col, val);
          count++;

          if (trackTime && count % 100000 == 0) {
            long stopTime = System.currentTimeMillis();
            if (startTime - stopTime > 1000*60) {
              System.out.printf("Ingest: %9d cnt, %6d secs, %8d entries/sec\n", count, (stopTime - origStartTime)/1000,
                  Math.round(count / ((stopTime - origStartTime)/1000.0)));
              startTime = stopTime;
            }
          }
        }
      }
    } catch (IOException e) {
      log.warn("",e);
      throw new RuntimeException(e);
    } finally {
      if (valScanner != null)
        valScanner.close();
    }
    return count;
  }

}
