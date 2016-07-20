package edu.mit.ll.graphulo_ocean;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.util.StatusLogger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_BYTES;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * The Accumulo column qualifier is the file name.
 * The Accumulo row is the kmer.
 */
public final class CSVIngesterKmer {
  private static final Logger log = LogManager.getLogger(CSVIngesterKmer.class);

  private final int K;
  private final GenomicEncoder G;
  private final Connector connector;

  public CSVIngesterKmer(Connector connector, int K) {
    this.connector = connector;
    this.K = K;
    G = new GenomicEncoder(K);
  }

//  static final Text EMPTY_TEXT = new Text();

  private static final class ArrayHolder implements Comparable<ArrayHolder> {
    public final byte[] b;

    public ArrayHolder(byte[] b) {
      this.b = b;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ArrayHolder that = (ArrayHolder) o;
      return Arrays.equals(b, that.b);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(b);
    }

    @Override
    public int compareTo(ArrayHolder o) {
      return WritableComparator.compareBytes(b, 0, b.length, o.b, 0, o.b.length);
    }
  }


  protected long ingestLine(Text row, BatchWriter bw, String line, SortedMap<ArrayHolder,Integer> map) {
//    String[] parts = line.split(",");
    int comma = line.indexOf(',');
    if (comma == -1) {
      log.error("Bad CSV line: "+line);
      return 0;
    }

//    String seqid = parts[0];
    String seq = line.substring(comma+1);
    char[] seqb = seq.toCharArray();

    // split seq into kmers
    long num = 0;
    int i = 0;

    // skip 'N' logic
    outer: while (true) {
      for (int j = i; j < i + K; j++) {
        if (seqb[j] == 'N') {
          i = j + 1;
          continue outer;
        }
      }
      break;
    }

    for (; i < seqb.length-K; i++) {
      if (seqb[i + K - 1] == 'N') {
        i += K;
        outer: while (true) {
          for (int j = i; j < i + K; j++) {
            if (seqb[j] == 'N') {
              i = j + 1;
              continue outer;
            }
          }
          break;
        }
      }
      byte[] e = G.encode(seqb, i);
      ArrayHolder ah = new ArrayHolder(e);
      Integer curval = map.get(ah);
      int newval = curval == null ? 1 : curval+1;
      map.put(ah, newval);
      num++;
    }

    return num;
  }


  public long ingestFile(File file, String Atable, boolean deleteIfExists) throws IOException {
    return ingestFile(file, Atable, deleteIfExists, 1, 0);
  }

  public long ingestFile(File file, String Atable, boolean deleteIfExists,
                         int everyXLines, int startOffset) throws IOException {
    Preconditions.checkArgument(everyXLines >= 1 && startOffset >= 0, "bad params ", everyXLines, startOffset);
    if (deleteIfExists && connector.tableOperations().exists(Atable))
      try {
        connector.tableOperations().delete(Atable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.warn("trouble deleting table "+Atable, e);
        throw new RuntimeException(e);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
    if (!connector.tableOperations().exists(Atable))
      try {
        connector.tableOperations().create(Atable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.warn("trouble creating table " + Atable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        throw new RuntimeException(e);
      }

    String sampleid0 = file.getName();
    if (sampleid0.endsWith(".csv"))
      sampleid0 = sampleid0.substring(0, sampleid0.length()-4);
    Text sampleid = new Text(sampleid0);
    byte[] sampleidb = sampleid0.getBytes(UTF_8);

    BatchWriter bw = null;
    String line = null;
    long entriesProcessed = 0, ingested = 0;

    try (BufferedReader fo = new BufferedReader(new FileReader(file))) {
      BatchWriterConfig config = new BatchWriterConfig();
      bw = connector.createBatchWriter(Atable, config);

      // Skip header line
      fo.readLine();
      // Offset
      for (int i = 0; i < startOffset; i++)
        fo.readLine();

      // log every 2 minutes
      StatusLogger slog = new StatusLogger();
      String partialMsg = file.getName()+": entries processed: ";

      SortedMap<ArrayHolder,Integer> map = new TreeMap<>();
      Lexicoder<Long> uil = new LongLexicoder();

      long linecnt = 0;
      while ((line = fo.readLine()) != null)
        if (!line.isEmpty() && linecnt++ % everyXLines == 0) {
          entriesProcessed += ingestLine(sampleid, bw, line, map);
          if (linecnt % 5 == 0)
            slog.logPeriodic(log, partialMsg+entriesProcessed);
        }

      log.info("Finished putting "+sampleid0+" into an in-memory map; now starting ingest");
      partialMsg = sampleid0+": entries ingested: ";

      ingested = 0;
      for (Map.Entry<ArrayHolder, Integer> entry : map.entrySet()) {
        Mutation m = new Mutation(entry.getKey().b);
        m.put(EMPTY_BYTES, sampleidb, uil.encode(entry.getValue().longValue()));
        bw.addMutation(m);
        ingested++;
        if (linecnt % 5 == 0)
          slog.logPeriodic(log, partialMsg+ingested);
      }


    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    } catch (MutationsRejectedException e) {
      log.warn("", e);
    } finally {
      if (bw != null)
        try {
          bw.close();
        } catch (MutationsRejectedException e) {
          log.warn("Mutation rejected at close() on line "+line, e);
        }
    }
    return ingested;
  }



}
