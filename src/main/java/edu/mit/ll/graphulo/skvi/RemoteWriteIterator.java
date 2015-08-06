package edu.mit.ll.graphulo.skvi;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.reducer.Reducer;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.PeekingIterator1;
import edu.mit.ll.graphulo.util.RangeSet;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * SKVI that writes to an Accumulo table.
 * Does all work in seek() method. hasTop() is always false.
 */
public class RemoteWriteIterator implements OptionDescriber, SortedKeyValueIterator<Key, Value> {
  private static final Logger log = LogManager.getLogger(RemoteWriteIterator.class);

  static final int REJECT_FAILURE_THRESHOLD = 10;
  private int numRejects = 0;

  private SortedKeyValueIterator<Key, Value> source;
  private String instanceName;
  private String tableName;
  private String tableNameTranspose;
  private String zookeeperHost;
  private String username;
  private AuthenticationToken auth;
  /**
   * Zookeeper timeout in milliseconds
   */
  private int timeout = -1;
  private int numEntriesCheckpoint = -1;
  /** Reduce-like functionality. */
  private Reducer reducer = new NOOP_REDUCER();
  static final class NOOP_REDUCER implements Reducer {
    @Override
    public void init(Map<String,String> options, IteratorEnvironment env) throws IOException {}
    @Override
    public void reset() throws IOException {}
    @Override
    public void update(Key k, Value v) {}
    @Override
    public void combine(byte[] another) {}
    @Override
    public boolean hasTopForClient() { return false; }
    @Override
    public byte[] getForClient() { return null; }
  }
  private Map<String,String> reducerOptions = new HashMap<>();

  /**
   * Created in init().
   */
  private MultiTableBatchWriter writerAll;
  private BatchWriter writer;
  private BatchWriter writerTranspose;
  /**
   * # entries written so far. Reset to 0 at writeUntilSave().
   */
  private int entriesWritten;
  /**
   * The last key returned by the source's safe state, or the seekRange's start key.
   */
  private Key lastSafeKey = new Key();

  /**
   * The range given by seek. Clip to this range.
   */
  private Range seekRange;
  /**
   * Set in options.
   */
  private RangeSet rowRanges = new RangeSet();
  /**
   * Holds the current range we are scanning.
   * Goes through the part of ranges after seeking to the beginning of the seek() clip.
   */
  private PeekingIterator1<Range> rowRangeIterator;
  private Collection<ByteSequence> seekColumnFamilies;
  private boolean seekInclusive;


  /**
   * Call init() after construction.
   */
  public RemoteWriteIterator() {
  }

  /**
   * Copies configuration from other, including connector,
   * EXCEPT creates a new, separate scanner.
   * No need to call init().
   */
  RemoteWriteIterator(RemoteWriteIterator other) {
    other.instanceName = instanceName;
    other.tableName = tableName;
    other.tableNameTranspose = tableNameTranspose;
    other.zookeeperHost = zookeeperHost;
    other.username = username;
    other.auth = auth;
    other.timeout = timeout;
    other.numEntriesCheckpoint = numEntriesCheckpoint;
    other.rowRanges = rowRanges;
    other.setupConnectorWriter();
  }

  static final IteratorOptions iteratorOptions;

  static {
    Map<String, String> optDesc = new LinkedHashMap<>();
    optDesc.put("zookeeperHost", "address and port");
    optDesc.put("timeout", "Zookeeper timeout between 1000 and 300000 (default 1000)");
    optDesc.put("instanceName", "");
    optDesc.put("tableName", "(optional) To write entries to.");
    optDesc.put("tableNameTranspose", "(optional) To write entries with row and column qualifier swapped.");
    optDesc.put("username", "");
    optDesc.put("password", "(Anyone who can read the Accumulo table config OR the log files will see your password in plaintext.)");
    optDesc.put("numEntriesCheckpoint", "(optional) #entries until sending back a progress monitor");
    optDesc.put("reducer", "(default does nothing) reducing function");
    optDesc.put("rowRanges", "(optional) rows to seek to");
    iteratorOptions = new IteratorOptions("RemoteWriteIterator",
        "Write to a remote Accumulo table.",
        Collections.unmodifiableMap(optDesc), null);
  }

  @Override
  public IteratorOptions describeOptions() {
    return iteratorOptions;
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    return validateOptionsStatic(options);
  }

  public static boolean validateOptionsStatic(Map<String, String> options) {
    new RemoteWriteIterator().parseOptions(options);
    return true;
  }

  @SuppressWarnings("unchecked")
  private void parseOptions(Map<String, String> map) {
    for (Map.Entry<String, String> optionEntry : map.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
      if (optionKey.startsWith("reducer.opt.")) {
        String keyAfterPrefix = optionKey.substring("reducer.opt.".length());
        reducerOptions.put(keyAfterPrefix, optionValue);
      } else {
        switch (optionKey) {
          case "zookeeperHost":
            zookeeperHost = optionValue;
            break;
          case "timeout":
            timeout = Integer.parseInt(optionValue);
            break;
          case "instanceName":
            instanceName = optionValue;
            break;
          case "tableName":
            tableName = optionValue;
            if (tableName.isEmpty())
              tableName = null;
            break;
          case "tableNameTranspose":
            tableNameTranspose = optionValue;
            if (tableNameTranspose.isEmpty())
              tableNameTranspose = null;
            break;
          case "username":
            username = optionValue;
            break;
          case "password":
            auth = new PasswordToken(optionValue);
            break;

          case "reducer":
            reducer = GraphuloUtil.subclassNewInstance(optionValue, Reducer.class);
            break;

          case "numEntriesCheckpoint":
            numEntriesCheckpoint = Integer.parseInt(optionValue);
            break;

          case "rowRanges":
            rowRanges.setTargetRanges(parseRanges(optionValue));
            break;

//          case "trace":
//            Watch.enableTrace = Boolean.parseBoolean(optionValue);
//            break;
          default:
            log.warn("Unrecognized option: " + optionEntry);
            continue;
        }
        log.trace("Option OK: " + optionEntry);
      }
    }
    // Required options
    if (//(tableName == null && tableNameTranspose == null && reducer.getClass().equals(NOOP_REDUCER.class)) ||
        // ^ Allowing case where RWI just acts as a RowRangeFilter / column range filter effectively
        ((tableName != null || tableNameTranspose != null) &&
            (zookeeperHost == null ||
                instanceName == null ||
                username == null ||
                auth == null)))
      throw new IllegalArgumentException("not enough options provided");
  }

  /**
   * Parse string s in the Matlab format "row1,row5,row7,:,row9,w,:,z,zz,:,"
   * Does not have to be ordered but cannot overlap.
   *
   * @param s -
   * @return a bunch of ranges
   */
  static SortedSet<Range> parseRanges(String s) {
    SortedSet<Range> rngs = GraphuloUtil.d4mRowToRanges(s);
    return new TreeSet<>(Range.mergeOverlapping(rngs));
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> map, IteratorEnvironment iteratorEnvironment) throws IOException {
//    for (Map.Entry<String, String> entry : iteratorEnvironment.getConfig()) {
//      System.out.println(entry.getKey() + " -> "+entry.getValue());
//    }

    this.source = source;
    if (source == null)
      throw new IllegalArgumentException("source must be specified");

//    Watch<Watch.PerfSpan> watch = Watch.getInstance();
//    watch.resetAll();
//    System.out.println("reset watch at RemoteWriteIterator init");

    parseOptions(map);

    if (!(source instanceof SaveStateIterator)) {
      numEntriesCheckpoint = -2; // disable save state
    }

    reducer.init(reducerOptions, iteratorEnvironment);

    setupConnectorWriter();

//    log.debug("RemoteWriteIterator on table " + tableName + ": init() succeeded");
  }

  private void setupConnectorWriter() {
    if (tableName == null && tableNameTranspose == null)
      return;

    ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeeperHost);
    if (timeout != -1)
      cc = cc.withZkTimeout(timeout);
    Instance instance = new ZooKeeperInstance(cc);
    Connector connector;
    try {
      connector = instance.getConnector(username, auth);
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.error("failed to connect to Accumulo instance " + instanceName, e);
      throw new RuntimeException(e);
    }

    BatchWriterConfig bwc = new BatchWriterConfig();
    // TODO: consider max memory, max latency, timeout, ... on writer

    if (tableName != null && tableNameTranspose != null)
      writerAll = connector.createMultiTableBatchWriter(bwc);
    else
      writerAll = null;

    try {
      if (tableName != null)
        writer = writerAll == null ? connector.createBatchWriter(tableName, bwc) : writerAll.getBatchWriter(tableName);
      if (tableNameTranspose != null)
        writerTranspose = writerAll == null ? connector.createBatchWriter(tableNameTranspose, bwc) : writerAll.getBatchWriter(tableNameTranspose);
    } catch (TableNotFoundException e) {
      log.error(tableName + " or " + tableNameTranspose + " does not exist in instance " + instanceName, e);
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException | AccumuloException e) {
      log.error("problem creating BatchWriters for " + tableName + " and " + tableNameTranspose);
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    log.info("finalize() RemoteWriteIterator " + tableName);
    System.out.println("finalize() RemoteWriteIterator " + tableName);
    if (writerAll != null)
      writerAll.close();
    else {
      if (writer != null)
        writer.close();
      if (writerTranspose != null)
        writerTranspose.close();
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    log.debug("RemoteWrite on table " + tableName + " / "+tableNameTranspose+" seek(): " + range);
    //System.out.println("RW passed seek " + range + "(thread " + Thread.currentThread().getName() + ")");

    reducer.reset();
//    boolean initialSeek = seekRange == null;
    seekRange = range;
    seekColumnFamilies = columnFamilies;
    seekInclusive = inclusive;
    rowRangeIterator = rowRanges.iteratorWithRangeMask(seekRange);
    writeWrapper(true/*, initialSeek*/);
  }

  private boolean writeWrapper(boolean doSeekNext/*, boolean initialSeek*/) throws IOException {
    boolean stoppedAtSafe = false;
    entriesWritten = 0;
    try {
      // while we have more ranges to seek
      // seek source to the next one and writeUntilSafeOrFinish()
      while (rowRangeIterator.hasNext()) {
        if (doSeekNext) {
          Range thisTargetRange = rowRangeIterator.peek();
          assert thisTargetRange.clip(seekRange, true) != null : "problem with RangeSet iterator intersecting seekRange";
          if (thisTargetRange.getStartKey() != null && thisTargetRange.getStartKey().compareTo(lastSafeKey) > 0)
            lastSafeKey = new Key(thisTargetRange.getStartKey());
          log.debug("RemoteWrite actual seek " + thisTargetRange);// + "(thread " + Thread.currentThread().getName() + ")");
          // We could use the 10x next() heuristic here...
//          if (!initialSeek)
//          seekNextHeuristic(thisTargetRange);
          source.seek(thisTargetRange, seekColumnFamilies, seekInclusive);
        }
        doSeekNext = true;
        stoppedAtSafe = writeUntilSafeOrFinish();
        if (stoppedAtSafe)
          break;
        rowRangeIterator.next();
      }
    } finally {
      // send reducer entries, if any present
      // flush anything written
      if (entriesWritten > 0) {
//        Watch<Watch.PerfSpan> watch = Watch.getInstance();
//        watch.start(Watch.PerfSpan.WriteFlush);
        try {
          if (writerAll != null)
            writerAll.flush();
          else {
            if (writer != null)
              writer.flush();
            if (writerTranspose != null)
              writerTranspose.flush();
          }
        } catch (MutationsRejectedException e) {
          log.warn("ignoring rejected mutations; ", e);
        }
//        finally {
//          watch.stop(Watch.PerfSpan.WriteFlush);
//          watch.print();
//        }
      }
    }
    return stoppedAtSafe;
  }

  /**
   * Return true if we stopped at a safe state with more entries to write, or
   * return false if no more entries to write (even if stopped at a safe state).
   */
  private boolean writeUntilSafeOrFinish() throws IOException {
    Mutation m;
//    Watch<Watch.PerfSpan> watch = Watch.getInstance();
    while (source.hasTop()) {
      Key k = source.getTopKey();
      Value v = source.getTopValue();

      reducer.update(k, v);

      if (writer != null) {
        m = new Mutation(k.getRowData().getBackingArray());
        m.put(k.getColumnFamilyData().getBackingArray(), k.getColumnQualifierData().getBackingArray(),
            k.getColumnVisibilityParsed(), v.get()); // no ts? System.currentTimeMillis()
//        watch.start(Watch.PerfSpan.WriteAddMut);
        try {
          writer.addMutation(m);
        } catch (MutationsRejectedException e) {
          numRejects++;
          log.warn("rejected mutations #"+numRejects+"; last one added is " + m, e);
        }
//        finally {
//          watch.stop(Watch.PerfSpan.WriteAddMut);
//        }
      }
      if (writerTranspose != null) {
        m = new Mutation(k.getColumnQualifierData().getBackingArray());
        m.put(k.getColumnFamilyData().getBackingArray(), k.getRowData().getBackingArray(),
            k.getColumnVisibilityParsed(), v.get()); // no ts? System.currentTimeMillis()
//        watch.start(Watch.PerfSpan.WriteAddMut);
        try {
          writerTranspose.addMutation(m);
        } catch (MutationsRejectedException e) {
          numRejects++;
          log.warn("rejected mutations #"+numRejects+"; last one added is " + m, e);
        }
//        finally {
//          watch.stop(Watch.PerfSpan.WriteAddMut);
//        }
      }

      if (numRejects >= REJECT_FAILURE_THRESHOLD) { // declare global failure after 10 rejects
        // last entry emitted declares failure
        rowRangeIterator = new PeekingIterator1<>(Iterators.<Range>emptyIterator());
        reducer = new NOOP_REDUCER();
        return true;
      }

      entriesWritten++;
      // check to see if we can save state
      if (numEntriesCheckpoint > 0 && entriesWritten >= numEntriesCheckpoint) {
        Key safeKey = ((SaveStateIterator) source).safeState();
        if (safeKey != null) {
          lastSafeKey = new Key(safeKey);
          return true;
        }
      }

//      watch.start(Watch.PerfSpan.WriteGetNext);
//      try {
        source.next();
//      } finally {
//        watch.stop(Watch.PerfSpan.WriteGetNext);
//      }
    }
    return false;
  }


  @Override
  public boolean hasTop() {
    return numRejects != -1 &&
        (numRejects >= REJECT_FAILURE_THRESHOLD ||
        rowRangeIterator.hasNext() ||
            entriesWritten > 0 ||
        //source.hasTop() ||
        reducer.hasTopForClient());
  }

  @Override
  public void next() throws IOException {
    if (numRejects >= REJECT_FAILURE_THRESHOLD)
      numRejects = -1;
    reducer.reset();
    if (entriesWritten > 0 || rowRangeIterator.hasNext()) {
      if (source.hasTop()) {
//        Watch<Watch.PerfSpan> watch = Watch.getInstance();
//        watch.start(Watch.PerfSpan.WriteGetNext);
//        try {
          source.next();
//        } finally {
//          watch.stop(Watch.PerfSpan.WriteGetNext);
//        }
        writeWrapper(false);
      } else {
        rowRangeIterator.next();
        writeWrapper(true);
      }
    }
  }

  @Override
  public Key getTopKey() {
    if (source.hasTop())
      return lastSafeKey;
    else
      return lastSafeKey.followingKey(PartialKey.ROW_COLFAM_COLQUAL);
  }

  @Override
  public Value getTopValue() {
    if (numRejects >= REJECT_FAILURE_THRESHOLD) {
      byte[] orig = REJECT_MESSAGE;
      ByteBuffer bb = ByteBuffer.allocate(orig.length + 8 + 2);
      bb.putLong(entriesWritten)
          .putChar(',')
          .put(orig)
          .rewind();
      return new Value(bb);
    } else {
      byte[] orig = //((SaveStateIterator) source).safeState().getValue().get();
          reducer.hasTopForClient() ? reducer.getForClient() : new byte[0];
      ByteBuffer bb = ByteBuffer.allocate(orig.length + 8 + 2);
      bb.putLong(entriesWritten)
          .putChar(',')
          .put(orig)
          .rewind();
      //      log.debug("topValue entriesWritten: "+entriesWritten);
      return new Value(bb);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public RemoteWriteIterator deepCopy(IteratorEnvironment iteratorEnvironment) {
    RemoteWriteIterator copy = new RemoteWriteIterator(this);
    copy.source = source.deepCopy(iteratorEnvironment);
    copy.reducerOptions.putAll(reducerOptions);
      try {
        copy.reducer = reducer.getClass().newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      try {
        copy.reducer.init(reducerOptions, iteratorEnvironment);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    return copy;
  }

  static final byte[] REJECT_MESSAGE = "Server_BatchWrite_Entries_Rejected!".getBytes();

  /**
   * Use this method on entries retrieved from a scan with a RemoteWriteIterator attached.
   * Decodes the Values received into the number of entries processed by RemoteWriteIterator
   * and, if a Reducer is given, updates the Reducer with the byte[] emitted from the
   * RemoteWriteIterator's server-side Reducer's {@link Reducer#getForClient()} method.
   * @param v Value from Scanner or BatchScanner.
   * @param reducer Calls combine() on the element inside if present. Pass null to ignore elements if any returned.
   * @return Number of entries seen by the RemoteWriteIterator
   */
  public static long decodeValue(Value v, Reducer reducer) {
    ByteBuffer bb = ByteBuffer.wrap(v.get());
    long numEntries = bb.getLong();
    bb.getChar(); // ','
    if (reducer != null && bb.hasRemaining())  {
      byte[] rest = new byte[bb.remaining()];
      if (Arrays.equals(REJECT_MESSAGE, rest)) {
        log.error("mutations rejected at server!");
      } else {
        bb.get(rest);
        reducer.combine(rest);
      }
    }
    return numEntries;
  }


}
