package edu.mit.ll.graphulo.d4m;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * Write row, column and (optionally) value files to a table.
 */
public class D4MTripleFileWriter {

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
   * @param trackTime Log the rate of ingest or not.
   * @return Number of triples written.
   * @throws FileNotFoundException
   */
  public long writeTripleFile(File rowFile, File colFile, File valFile,
                              String delimiter, String baseName,
                              boolean deleteExistingTables, boolean trackTime) throws FileNotFoundException {
    long count = 0, startTime, origStartTime;
    Text row = new Text(), col = new Text(), valText = null;
    Value val = D4MTableWriter.VALONE;

    Scanner valScanner = null;
    try (Scanner rowScanner = new Scanner(rowFile);
         Scanner colScanner = new Scanner(colFile)) {
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
      config.useTableDeg = true;
      config.useTableDegT = false;
      config.useTableField = false;
      config.useTableFieldT = false;
      config.connector = connector;

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
    } finally {
      if (valScanner != null)
        valScanner.close();
    }
    return count;
  }

}
