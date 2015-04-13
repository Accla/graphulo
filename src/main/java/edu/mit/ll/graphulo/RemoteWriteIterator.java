package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
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

  private SortedKeyValueIterator<Key, Value> source;
  private String instanceName;
  private String tableName;
  private String zookeeperHost;
  private String username;
  private AuthenticationToken auth;
  /**
   * Zookeeper timeout in milliseconds
   */
  private int timeout = -1;

  private int numEntriesCheckpoint = -1;

  /**
   * Created in init().
   */
  private BatchWriter writer;
  /**
   * # entries written so far. Reset to 0 at writeUntilSave().
   */
  private int entriesWritten;

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
  public RemoteWriteIterator(RemoteWriteIterator other) {
    other.instanceName = instanceName;
    other.tableName = tableName;
    other.zookeeperHost = zookeeperHost;
    other.username = username;
    other.auth = auth;
    other.timeout = timeout;
    other.setupConnectorWriter();
  }

  static final IteratorOptions iteratorOptions;

  static {
    Map<String, String> optDesc = new LinkedHashMap<>();
    optDesc.put("zookeeperHost", "address and port");
    optDesc.put("timeout", "Zookeeper timeout between 1000 and 300000 (default 1000)");
    optDesc.put("instanceName", "");
    optDesc.put("tableName", "");
    optDesc.put("username", "");
    optDesc.put("password", "(Anyone who can read the Accumulo table config OR the log files will see your password in plaintext.)");
    optDesc.put("numEntriesCheckpoint", "(optional) #entries until sending back a progress monitor");
    iteratorOptions = new IteratorOptions("RemoteWriteIterator",
        "Write to a remote Accumulo table. hasTop() is always false.",
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
    String zookeeperHost = null, instanceName = null, tableName = null, username = null;
    AuthenticationToken auth = null;
    //int timeout;

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

        default:
          throw new IllegalArgumentException("unknown option: " + entry);
      }
    }
    // Required options
    if (zookeeperHost == null ||
        instanceName == null ||
        tableName == null ||
        username == null ||
        auth == null)
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

        default:
          log.warn("Unrecognized option: " + entry);
          continue;
      }
      log.trace("Option OK: " + entry);
    }
    // Required options
    if (zookeeperHost == null ||
        instanceName == null ||
        tableName == null ||
        username == null ||
        auth == null)
      throw new IllegalArgumentException("not enough options provided");
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> map, IteratorEnvironment iteratorEnvironment) throws IOException {
    this.source = source;
    if (source == null)
      throw new IllegalArgumentException("source must be specified");

    parseOptions(map);

    if (!(source instanceof SaveStateIterator)) {
      numEntriesCheckpoint = -2; // disable save state
    }

    setupConnectorWriter();

    log.debug("RemoteWriteIterator on table " + tableName + ": init() succeeded");
  }

  private void setupConnectorWriter() {
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

    try {
      writer = connector.createBatchWriter(tableName, bwc);
    } catch (TableNotFoundException e) {
      log.error(tableName + " does not exist in instance " + instanceName, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void finalize() throws Throwable {
    log.info("finalize() called on RemoteWriteIterator for table " + tableName);
    writer.close();
    super.finalize();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    log.debug("RemoteWriteIterator on table " + tableName + ": about to seek() to range " + range);
    source.seek(range, columnFamilies, inclusive);

    writeUntilSafeOrFinish();
  }

  private void writeUntilSafeOrFinish() throws IOException {
    Mutation m = null;
    entriesWritten = 0;
    try {
      while (source.hasTop()) {
        Key k = source.getTopKey();
        Value v = source.getTopValue();
        m = new Mutation(k.getRowData().getBackingArray());
        m.put(k.getColumnFamilyData().getBackingArray(), k.getColumnQualifierData().getBackingArray(),
            k.getColumnVisibilityParsed(), v.get()); // no ts? System.currentTimeMillis()
        Watch.instance.start(Watch.PerfSpan.WriteAddMut);
        try {
          writer.addMutation(m);
        } catch (MutationsRejectedException e) {
          log.warn("ignoring rejected mutations; last one added is " + m, e);
        } finally {
          Watch.instance.stop(Watch.PerfSpan.WriteAddMut);
        }

        entriesWritten++;
        // check to see if we can save state
        if (numEntriesCheckpoint > 0 && entriesWritten >= numEntriesCheckpoint) {
          Map.Entry<Key, Value> safeState = ((SaveStateIterator) source).safeState();
          if (safeState != null) {
            break;
          }
        }

        Watch.instance.start(Watch.PerfSpan.WriteGetNext);
        try {
          source.next();
        } finally {
          Watch.instance.stop(Watch.PerfSpan.WriteGetNext);
        }
      }
    } finally {
      Watch.instance.start(Watch.PerfSpan.WriteFlush);
      try {
        writer.flush();
      } catch (MutationsRejectedException e) {
        log.warn("ignoring rejected mutations; "
            + (m == null ? "none added so far (?)" : "last one added is " + m), e);
      } finally {
        Watch.instance.stop(Watch.PerfSpan.WriteFlush);
      }
    }
  }


  @Override
  public boolean hasTop() {
    assert source instanceof SaveStateIterator && ((SaveStateIterator) source).safeState() != null
        : source + " is not a SaveStateIterator or not at a safe state.";
    return source.hasTop();
  }

  @Override
  public void next() throws IOException {
    Watch.instance.start(Watch.PerfSpan.WriteGetNext);
    try {
      source.next();
    } finally {
      Watch.instance.stop(Watch.PerfSpan.WriteGetNext);
    }

    writeUntilSafeOrFinish();
  }

  @Override
  public Key getTopKey() {
    assert source instanceof SaveStateIterator && ((SaveStateIterator) source).safeState() != null
        : source + " is not a SaveStateIterator or not at a safe state.";
    return ((SaveStateIterator) source).safeState().getKey();
  }

  @Override
  public Value getTopValue() {
    assert source instanceof SaveStateIterator && ((SaveStateIterator) source).safeState() != null
        : source + " is not a SaveStateIterator or not at a safe state.";
    Value orig = ((SaveStateIterator) source).safeState().getValue();
    ByteBuffer bb = ByteBuffer.allocate(orig.getSize() + 4 + 2);
    bb.putInt(entriesWritten)
        .putChar(',')
        .put(orig.get())
        .rewind();
    return new Value(bb);
  }

  @Override
  public RemoteWriteIterator deepCopy(IteratorEnvironment iteratorEnvironment) {
//        RemoteWriteIterator copy =  new RemoteWriteIterator(this);
//        copy.source = source.deepCopy(iteratorEnvironment);
//        return copy;
    throw new UnsupportedOperationException();
  }


}
