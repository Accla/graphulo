package edu.mit.ll.graphulo_ocean;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.skvi.LongLexicoderTemp;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.StatusLogger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.commons.math3.util.Pair;
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
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

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
  private final boolean alsoIngestReverseComplement;

  public CSVIngesterKmer(Connector connector, int K) {
    this(connector, K, false, false);
  }

  public CSVIngesterKmer(Connector connector, int K, boolean alsoIngestReverseComplement, boolean alsoIngestSmallerLex) {
    this.connector = connector;
    this.alsoIngestSmallerLex = alsoIngestSmallerLex;
    this.K = K;
    G = new GenomicEncoder(K);
    this.alsoIngestReverseComplement = alsoIngestReverseComplement;
  }

  //  static final Text EMPTY_TEXT = new Text();

  private static final class ArrayHolder implements Comparable<ArrayHolder> {
    public final byte[] b;

    ArrayHolder(byte[] b) {
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


  private long ingestLine(String line, SortedMap<ArrayHolder,Integer> map, SortedMap<ArrayHolder,Integer> mapRC, SortedMap<ArrayHolder,Integer> mapSmaller) {
//    String[] parts = line.split(",");
    int comma = line.indexOf(',');
    if (comma == -1) {
      log.warn("Bad CSV line: "+line);
      return 0;
    }

    // Find the '/' character before the comma.
    // If the character after it is '2', then take the reverse complement.
    String header = line.substring(0, comma-1);
    int slash = header.indexOf('/');
    boolean reverse = slash != -1 && slash != header.length()-1 && header.charAt(slash+1) == '2';

//    String seqid = parts[0];
    String seq = line.substring(comma+1);
    char[] seqb = seq.toCharArray();

    // split seq into kmers
    long num = 0;
    int i = 0;

    // skip 'N' logic
    outer: while (true) {
      for (int j = i; j < i + K; j++)
        if (seqb[j] == 'N') {
          i = j + 1;
          continue outer;
        }
      break;
    }

    for (; i < seqb.length-K; i++) {
      if (seqb[i + K - 1] == 'N') {
        i += K;
        outer: while (true) {
          for (int j = i; j < Math.min(i + K, seqb.length); j++)
            if (seqb[j] == 'N') {
              i = j + 1;
              continue outer;
            }
          break;
        }
        i--; continue;
      }

      byte[] e = G.encode(seqb, i);
      putInMap(map, e);

      if (alsoIngestReverseComplement || alsoIngestSmallerLex) {
        byte[] eo = Arrays.copyOf(e, e.length);
        G.reverseComplement(eo);
        if (alsoIngestReverseComplement) {
          putInMap(mapRC, eo);
        }
        if (alsoIngestSmallerLex) {
          putInMap(mapSmaller,
              WritableComparator.compareBytes(e, 0, e.length, eo, 0, eo.length) <= 0
                  ? e : eo);
        }
      }
      num++;
    }
    return num;
  }

  private void putInMap(Map<ArrayHolder, Integer> map, byte[] e) {
    ArrayHolder ah = new ArrayHolder(e);
    Integer curval = map.get(ah);
    int newval = curval == null ? 1 : curval + 1;
    map.put(ah, newval);
  }


  public long ingestFile(File file, String Atable, boolean deleteIfExists, String oTsampleDegree) throws IOException {
    return ingestFile(file, Atable, deleteIfExists, 1, 0, oTsampleDegree);
  }

  private boolean alsoIngestSmallerLex = false;

  private SortedSet<Text> getSplitPoints(int power4splits) {
    SortedSet<Text> set = new TreeSet<>();
    Preconditions.checkArgument(power4splits < 8);
    for (byte i = 1; i < 1 << power4splits; i++) {
      byte[] ba = new byte[1];
      ba[0] = (byte) (i << (8 - power4splits));
      set.add(new Text( ba ));
    }
    return set;
  }

  private void createSeqTable(String table) {
    // create tables if they don't exist
    if (!connector.tableOperations().exists(table)) {

      IteratorSetting longCombiner = new IteratorSetting(1, SummingCombiner.class);
      SummingCombiner.setCombineAllColumns(longCombiner, true);
      SummingCombiner.setEncodingType(longCombiner, LongLexicoderTemp.class);

      try {
        connector.tableOperations().create(table);
        connector.tableOperations().addSplits(table, getSplitPoints(3));
        GraphuloUtil.applyIteratorSoft(longCombiner, connector.tableOperations(), table);

      } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | TableExistsException e) {
        log.warn("", e);
      }
    }
  }

  private void createDegreeTable(String table) {
    // create tables if they don't exist
    if (!connector.tableOperations().exists(table)) {

      IteratorSetting longCombiner = Graphulo.PLUS_ITERATOR_LONG;

      try {
        connector.tableOperations().create(table);
        GraphuloUtil.applyIteratorSoft(longCombiner, connector.tableOperations(), table);

      } catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
        log.warn("", e);
      }
    }
  }

  public long ingestFile(File file, String Atable, boolean deleteIfExists,
                         int everyXLines, int startOffset, String oTsampleDegree) throws IOException {
    Preconditions.checkArgument(everyXLines >= 1 && startOffset >= 0, "bad params ", everyXLines, startOffset);

    createSeqTable(Atable);
    createDegreeTable(oTsampleDegree);
    String AtableRC = Atable+"_RC", AtableSmaller = Atable+"_Smaller";
    if (alsoIngestReverseComplement)
      createSeqTable(AtableRC);
    if (alsoIngestSmallerLex)
      createSeqTable(AtableSmaller);

    String sampleid0 = file.getName();
    if (sampleid0.endsWith(".csv"))
      sampleid0 = sampleid0.substring(0, sampleid0.length()-4);
    byte[] sampleidb = sampleid0.getBytes(UTF_8);

    MultiTableBatchWriter mtbw = null;
    String line = null;
    long entriesProcessed = 0, ingested = 0, totalsum = 0;

    BatchWriterConfig config = new BatchWriterConfig();
    try (BufferedReader fo = new BufferedReader(new FileReader(file))) {
      BatchWriter bw, bwRC, bwSmaller;
      mtbw = connector.createMultiTableBatchWriter(config);
      bw = mtbw.getBatchWriter(Atable);
      bwRC = alsoIngestReverseComplement ? mtbw.getBatchWriter(AtableRC) : null;
      bwSmaller = alsoIngestSmallerLex ? mtbw.getBatchWriter(AtableSmaller) : null;

      // Skip header line
      fo.readLine();
      // Offset
      for (int i = 0; i < startOffset; i++)
        fo.readLine();

      // log every 2 minutes
      StatusLogger slog = new StatusLogger();
      String partialMsg = file.getName()+": entries processed: ";

      // idea: replace with a fixed-size array. Re-use between results
      SortedMap<ArrayHolder,Integer> map = new TreeMap<>(),
          mapRC = alsoIngestReverseComplement ? new TreeMap<ArrayHolder,Integer>() : null,
          mapSmaller = alsoIngestSmallerLex ? new TreeMap<ArrayHolder,Integer>() : null;

      long linecnt = 0;
      while ((line = fo.readLine()) != null)
        if (!line.isEmpty() && linecnt++ % everyXLines == 0) {
          entriesProcessed += ingestLine(line, map, mapRC, mapSmaller);
          if (linecnt % 5 == 0)
            slog.logPeriodic(log, partialMsg+entriesProcessed);
        }

      log.info("Finished putting "+sampleid0+" into an in-memory map; now starting ingest");
      partialMsg = sampleid0+": entries ingested: ";


      Pair<Long, Long> p = ingestMap(map, bw, sampleidb);
      log.info("wrote "+p.getSecond()+" forward entries to "+Atable);
      ingested = p.getFirst();
      totalsum = p.getSecond();

      p = alsoIngestReverseComplement ? ingestMap(mapRC, bwRC, sampleidb) : p;
      if (alsoIngestReverseComplement)
        log.info("wrote "+p.getSecond()+" reverse complement entries to "+AtableRC);
      if (ingested != p.getFirst() || p.getSecond() != totalsum)
        log.warn("RC "+p.getFirst()+" not equal to ingested "+ingested+" or totalsum "+p.getSecond()+" not equal to totalsum "+totalsum);

      p = alsoIngestSmallerLex ? ingestMap(mapSmaller, bwSmaller, sampleidb) : p;
      if (alsoIngestSmallerLex)
        log.info("wrote "+p.getSecond()+" smaller lex entries to "+AtableSmaller);
      if (ingested != p.getFirst() || p.getSecond() != totalsum)
        log.warn("SMALLER "+p.getFirst()+" not equal to ingested "+ingested+" or totalsum "+p.getSecond()+" not equal to totalsum "+totalsum);


      // write degree
      BatchWriter bwd = mtbw.getBatchWriter(oTsampleDegree);
      Mutation m = new Mutation(sampleidb);
      m.put(EMPTY_BYTES, ("degree").getBytes(UTF_8), (""+totalsum).getBytes(UTF_8));
      bwd.addMutation(m);
      log.info("wrote "+totalsum+" as degree of "+sampleid0+" to "+oTsampleDegree);

    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException | AccumuloException e) {
      log.warn("", e);
    } finally {
      if (mtbw != null)
        try {
          mtbw.close();
        } catch (MutationsRejectedException e) {
          log.warn("Mutation rejected at close() on line "+line, e);
        }
    }

    return ingested;
  }

  private final static Lexicoder<Long> LEX = new LongLexicoder();

  private Pair<Long,Long> ingestMap(SortedMap<ArrayHolder, Integer> map, BatchWriter bw, byte[] sampleidb) throws MutationsRejectedException {
    long ingested = 0, totalsum = 0;
    for (Map.Entry<ArrayHolder, Integer> entry : map.entrySet()) {
      Mutation m = new Mutation(entry.getKey().b);
      long lv = entry.getValue().longValue();
      totalsum += lv;
      m.put(EMPTY_BYTES, sampleidb, LEX.encode(lv));
      bw.addMutation(m);
      ingested++;
    }
    return new Pair<>(ingested, totalsum);
  }


}
