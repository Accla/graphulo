package edu.mit.ll.graphulo;

import com.google.common.collect.Iterators;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

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
  private boolean gatherColQs = false;

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
   * Ensure that entry from setUniqueColQs comes after any monitor entries emitted.
   */
  private Key lastKeyEmitted = new Key();
  private HashSet<String> setUniqueColQs = null;

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
    other.setUniqueColQs = setUniqueColQs == null ? null : new HashSet<>(setUniqueColQs);
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
    optDesc.put("gatherColQs", "(default false) gather set of unique column qualifiers passed in and send back all of them at the end");
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
    // Shadow all the fields =)
    String zookeeperHost = null, instanceName = null, tableName = null, username = null, tableNameTranspose = null;
    AuthenticationToken auth = null;
    boolean gatherColQs = false;

    for (Map.Entry<String, String> entry : options.entrySet()) {
      switch (entry.getKey()) {
        case "zookeeperHost":
          zookeeperHost = entry.getValue();
          break;
        case "timeout":
          try {
            int t = Integer.parseInt(entry.getValue());
            if (t < 1000 || t > 300000)
              throw new IllegalArgumentException("timeout out of range [1000,300000]: " + t);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("bad timeout", e);
          }
          break;
        case "instanceName":
          instanceName = entry.getValue();
          break;
        case "tableName":
          tableName = entry.getValue();
          if (tableName.isEmpty())
            tableName = null;
          break;
        case "tableNameTranspose":
          tableNameTranspose = entry.getValue();
          if (tableNameTranspose.isEmpty())
            tableNameTranspose = null;
          break;
        case "username":
          username = entry.getValue();
          break;
        case "password":
          auth = new PasswordToken(entry.getValue());
          break;

        case "numEntriesCheckpoint":
          //noinspection ResultOfMethodCallIgnored
          Integer.parseInt(entry.getValue());
          break;
        case "gatherColQs":
          gatherColQs = Boolean.parseBoolean(entry.getValue());
          break;

        case "rowRanges":
          parseRanges(entry.getValue());
          break;

        default:
          throw new IllegalArgumentException("unknown option: " + entry);
      }
    }
    // Required options
    if ((tableName == null && tableNameTranspose == null && !gatherColQs) ||
        ((tableName != null || tableNameTranspose != null) &&
            (zookeeperHost == null ||
                instanceName == null ||
                username == null ||
                auth == null)))
      throw new IllegalArgumentException("not enough options provided");
    return true;
  }

  private void parseOptions(Map<String, String> map) {
    for (Map.Entry<String, String> entry : map.entrySet()) {
      switch (entry.getKey()) {
        case "zookeeperHost":
          zookeeperHost = entry.getValue();
          break;
        case "timeout":
          timeout = Integer.parseInt(entry.getValue());
          break;
        case "instanceName":
          instanceName = entry.getValue();
          break;
        case "tableName":
          tableName = entry.getValue();
          if (tableName.isEmpty())
            tableName = null;
          break;
        case "tableNameTranspose":
          tableNameTranspose = entry.getValue();
          if (tableNameTranspose.isEmpty())
            tableNameTranspose = null;
          break;
        case "username":
          username = entry.getValue();
          break;
        case "password":
          auth = new PasswordToken(entry.getValue());
          break;

        case "numEntriesCheckpoint":
          numEntriesCheckpoint = Integer.parseInt(entry.getValue());
          break;
        case "gatherColQs":
          gatherColQs = Boolean.parseBoolean(entry.getValue());
          break;

        case "rowRanges":
          rowRanges.setTargetRanges(parseRanges(entry.getValue()));
          break;

        default:
          log.warn("Unrecognized option: " + entry);
          continue;
      }
      log.trace("Option OK: " + entry);
    }
    // Required options
    if ((tableName == null && tableNameTranspose == null && !gatherColQs) ||
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
  private static SortedSet<Range> parseRanges(String s) {
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

    Watch<Watch.PerfSpan> watch = Watch.getInstance();
    watch.resetAll();
    System.out.println("reset watch at RemoteWriteIterator init");

    parseOptions(map);

    if (!(source instanceof SaveStateIterator)) {
      numEntriesCheckpoint = -2; // disable save state
    }
    if (gatherColQs)
      setUniqueColQs = new HashSet<>();

    setupConnectorWriter();

    log.debug("RemoteWriteIterator on table " + tableName + ": init() succeeded");
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
    log.debug("RemoteWrite on table " + tableName + " passed seek(): " + range);
    //System.out.println("RW passed seek " + range + "(thread " + Thread.currentThread().getName() + ")");

    seekRange = range;
    seekColumnFamilies = columnFamilies;
    seekInclusive = inclusive;
    rowRangeIterator = rowRanges.iteratorWithRangeMask(seekRange);
    writeWrapper(true);
  }

  private boolean writeWrapper(boolean doSeekNext) throws IOException {
    boolean stoppedAtSafe = false;
    entriesWritten = 0;
    try {
      // while we have more ranges to seek
      // seek source to the next one and writeUntilSafeOrFinish()
      while (rowRangeIterator.hasNext()) {
        if (doSeekNext) {
          Range thisTargetRange = rowRangeIterator.peek();
          assert thisTargetRange.clip(seekRange, true) != null : "problem with RangeSet iterator intersecting seekRange";
          if (thisTargetRange.getStartKey() != null && thisTargetRange.getStartKey().compareTo(lastKeyEmitted) > 0)
            lastKeyEmitted.set(thisTargetRange.getStartKey());
          log.debug("RemoteWrite actual seek " + thisTargetRange);// + "(thread " + Thread.currentThread().getName() + ")");
          // We could use the 10x next() heuristic here...
          source.seek(thisTargetRange, seekColumnFamilies, seekInclusive);
        }
        doSeekNext = true;
        stoppedAtSafe = writeUntilSafeOrFinish();
        if (stoppedAtSafe)
          break;
        rowRangeIterator.next();
      }
    } finally {
      // flush anything written
      if (entriesWritten > 0) {
        Watch<Watch.PerfSpan> watch = Watch.getInstance();
        watch.start(Watch.PerfSpan.WriteFlush);
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
        } finally {
          watch.stop(Watch.PerfSpan.WriteFlush);
          watch.print();
        }
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
    Watch<Watch.PerfSpan> watch = Watch.getInstance();
    while (source.hasTop()) {
      Key k = source.getTopKey();
      Value v = source.getTopValue();

      if (gatherColQs) {
        setUniqueColQs.add(new String(k.getColumnQualifierData().getBackingArray()));
      }

      if (writer != null) {
        m = new Mutation(k.getRowData().getBackingArray());
        m.put(k.getColumnFamilyData().getBackingArray(), k.getColumnQualifierData().getBackingArray(),
            k.getColumnVisibilityParsed(), v.get()); // no ts? System.currentTimeMillis()
        watch.start(Watch.PerfSpan.WriteAddMut);
        try {
          writer.addMutation(m);
        } catch (MutationsRejectedException e) {
          numRejects++;
          log.warn("rejected mutations #"+numRejects+"; last one added is " + m, e);
        } finally {
          watch.stop(Watch.PerfSpan.WriteAddMut);
        }
      }
      if (writerTranspose != null) {
        m = new Mutation(k.getColumnQualifierData().getBackingArray());
        m.put(k.getColumnFamilyData().getBackingArray(), k.getRowData().getBackingArray(),
            k.getColumnVisibilityParsed(), v.get()); // no ts? System.currentTimeMillis()
        watch.start(Watch.PerfSpan.WriteAddMut);
        try {
          writerTranspose.addMutation(m);
        } catch (MutationsRejectedException e) {
          numRejects++;
          log.warn("rejected mutations #"+numRejects+"; last one added is " + m, e);
        } finally {
          watch.stop(Watch.PerfSpan.WriteAddMut);
        }
      }

      if (numRejects >= REJECT_FAILURE_THRESHOLD) { // declare global failure after 10 rejects
        rowRangeIterator = new PeekingIterator1<>(Iterators.<Range>emptyIterator());
        // last entry emitted declares failure
        return true;
      }

      entriesWritten++;
      // check to see if we can save state
      if (numEntriesCheckpoint > 0 && entriesWritten >= numEntriesCheckpoint) {
        Map.Entry<Key, Value> safeState = ((SaveStateIterator) source).safeState();
        if (safeState != null) {
          lastKeyEmitted.set(safeState.getKey());
          return true;
        }
      }

      watch.start(Watch.PerfSpan.WriteGetNext);
      try {
        source.next();
      } finally {
        watch.stop(Watch.PerfSpan.WriteGetNext);
      }
    }
    return false;
  }


  @Override
  public boolean hasTop() {
    return numRejects >= REJECT_FAILURE_THRESHOLD ||
        rowRangeIterator.hasNext() ||
        //source.hasTop() ||
        (gatherColQs && !setUniqueColQs.isEmpty());
  }

  @Override
  public void next() throws IOException {
    if (numRejects >= REJECT_FAILURE_THRESHOLD)
      numRejects = -1;
    else if (rowRangeIterator.hasNext()) {
      if (source.hasTop()) {
        Watch<Watch.PerfSpan> watch = Watch.getInstance();
        watch.start(Watch.PerfSpan.WriteGetNext);
        try {
          source.next();
        } finally {
          watch.stop(Watch.PerfSpan.WriteGetNext);
        }
        writeWrapper(false);
      } else {
        rowRangeIterator.next();
        writeWrapper(true);
      }
    } else if (gatherColQs && !setUniqueColQs.isEmpty()) {
      setUniqueColQs.clear();
    } else
      throw new IllegalStateException();
  }

  @Override
  public Key getTopKey() {
    if (numRejects >= REJECT_FAILURE_THRESHOLD)
      return new Key(lastKeyEmitted).followingKey(PartialKey.ROW);
    else if (source.hasTop())
      return ((SaveStateIterator) source).safeState().getKey();
    else if (gatherColQs && !setUniqueColQs.isEmpty()) {
      return new Key(lastKeyEmitted).followingKey(PartialKey.ROW);
    } else
      return null;
  }

  @Override
  public Value getTopValue() {
    if (numRejects >= REJECT_FAILURE_THRESHOLD) {
      byte[] orig = ((SaveStateIterator) source).safeState().getValue().get();
      ByteBuffer bb = ByteBuffer.allocate(orig.length + 4 + 2);
      bb.putInt(entriesWritten)
          .putChar(',')
          .put("Server_BatchWrite_Entries_Rejected!".getBytes())
          .rewind();
      return new Value(bb);
    } else if (source.hasTop()) {
      byte[] orig = ((SaveStateIterator) source).safeState().getValue().get();
      ByteBuffer bb = ByteBuffer.allocate(orig.length + 4 + 2);
      bb.putInt(entriesWritten)
          .putChar(',')
          .put(orig)
          .rewind();
      return new Value(bb);
    } else if (gatherColQs && !setUniqueColQs.isEmpty()) {
      return new Value(SerializationUtils.serialize(setUniqueColQs));
    } else
      return null;
  }

  @Override
  public RemoteWriteIterator deepCopy(IteratorEnvironment iteratorEnvironment) {
    RemoteWriteIterator copy = new RemoteWriteIterator(this);
    copy.source = source.deepCopy(iteratorEnvironment);
    return copy;
  }


}
