package edu.mit.ll.graphulo.skvi;

import edu.mit.ll.graphulo.DynamicIteratorSetting;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.PeekingIterator1;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientSideIteratorScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Reads from a remote Accumulo table.
 */
public class RemoteSourceIterator implements SortedKeyValueIterator<Key, Value>/*, OptionDescriber*/ {
  private static final Logger log = LogManager.getLogger(RemoteSourceIterator.class);

  /** The original options passed to init. Retaining this makes deepCopy much easier-- call init again and done! */
  private Map<String,String> origOptions;

  private String instanceName;
  private String tableName;
  private String zookeeperHost;
  private String username;
  private AuthenticationToken auth;
  private Authorizations authorizations = Authorizations.EMPTY;
  /**
   * Zookeeper timeout in milliseconds
   */
  private int timeout = -1;

  private boolean doWholeRow = false,
      doClientSideIterators = false;
  private DynamicIteratorSetting dynamicIteratorSetting;
  private SortedSet<Range> rowRanges = new TreeSet<>(Collections.singleton(new Range()));
  /**
   * The range given by seek. Clip to this range.
   */
  private Range seekRange;

  /**
   * Holds the current range we are scanning.
   * Goes through the part of ranges after seeking to the beginning of the seek() clip.
   */
  private Iterator<Range> rowRangeIterator;
  private String colFilter = "";

  /**
   * Created in init().
   */
  private Scanner scanner;
  /**
   * Buffers one entry from the remote table.
   */
  private PeekingIterator1<Map.Entry<Key, Value>> remoteIterator;


  private void parseOptions(Map<String, String> map) {
    Map<String,String> diterMap = new HashMap<>();
    for (Map.Entry<String, String> optionEntry : map.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
      if (optionValue.isEmpty())
        continue;
      if (optionKey.startsWith("diter.")) {
        diterMap.put(optionKey.substring("diter.".length()), optionValue);
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
            break;
          case "username":
            username = optionValue;
            break;
          case "password":
            auth = new PasswordToken(optionValue);
            break;
          case "authorizations": // passed value must be from Authorizations.serialize()
            authorizations = new Authorizations(optionValue.getBytes(StandardCharsets.UTF_8));
            break;

          case "doWholeRow":
            doWholeRow = Boolean.parseBoolean(optionValue);
            break;
          case "rowRanges":
            rowRanges = parseRanges(optionValue);
            break;
          case "colFilter":
            colFilter = optionValue; //GraphuloUtil.d4mRowToTexts(optionValue);
            break;
          case "doClientSideIterators":
            doClientSideIterators = Boolean.parseBoolean(optionValue);
            break;
          default:
            log.warn("Unrecognized option: " + optionEntry);
            continue;
        }
      }
      log.trace("Option OK: " + optionEntry);
    }
    if (!diterMap.isEmpty())
      dynamicIteratorSetting = DynamicIteratorSetting.fromMap(diterMap);
    // Required options
    if (zookeeperHost == null ||
        instanceName == null ||
        tableName == null ||
        username == null ||
        auth == null)
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
    Collection<Range> rngs = GraphuloUtil.d4mRowToRanges(s);
    rngs = Range.mergeOverlapping(rngs);
    return new TreeSet<>(rngs);
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> map, IteratorEnvironment iteratorEnvironment) throws IOException {
    if (source != null)
      log.warn("RemoteSourceIterator ignores/replaces parent source passed in init(): " + source);
    origOptions = new HashMap<>(map); // defensive copy

    parseOptions(map);
    setupConnectorScanner();

    log.debug("RemoteSourceIterator on table " + tableName + ": init() succeeded");
  }

  private void setupConnectorScanner() {
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

    try {
      scanner = connector.createScanner(tableName, authorizations);
    } catch (TableNotFoundException e) {
      log.error(tableName + " does not exist in instance " + instanceName, e);
      throw new RuntimeException(e);
    }

    if (doClientSideIterators)
      scanner = new ClientSideIteratorScanner(scanner);

    if (dynamicIteratorSetting == null)
      GraphuloUtil.applyGeneralColumnFilter(colFilter,scanner,10);
    else {
      GraphuloUtil.applyGeneralColumnFilter(colFilter, scanner, dynamicIteratorSetting, false); // prepend
      scanner.addScanIterator(dynamicIteratorSetting.toIteratorSetting(10));
    }

    if (doWholeRow) { // This is a legacy setting.
      // TODO: make priority dynamic in case 25 is taken; make name dynamic in case iterator name already exists. Or buffer here.
      scanner.addScanIterator(new IteratorSetting(25, WholeRowIterator.class));
    }

  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    scanner.close();
  }

  /**
   * Advance to the first subset range whose end key >= the seek start key.
   */
  public static Iterator<Range> getFirstRangeStarting(PeekingIterator1<Range> iter, Range seekRange) {
    if (!seekRange.isInfiniteStartKey())
      while (iter.hasNext() && !iter.peek().isInfiniteStopKey()
          && ((iter.peek().getEndKey().equals(seekRange.getStartKey()) && !seekRange.isEndKeyInclusive())
          || iter.peek().getEndKey().compareTo(seekRange.getStartKey()) < 0)) {
        iter.next();
      }
    return iter;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    log.debug("RemoteSourceIterator on table " + tableName + ": seek(): " + range);
    /** configure Scanner to the first entry to inject after the start of the range.
     Range comparison: infinite start first, then inclusive start, then exclusive start
     {@link org.apache.accumulo.core.data.Range#compareTo(Range)} */
    seekRange = range;
    rowRangeIterator = getFirstRangeStarting(new PeekingIterator1<>(rowRanges.iterator()), range); //rowRanges.tailSet(range).iterator();
    remoteIterator = PeekingIterator1.emptyIterator();
    next();
  }

//  /**
//   * Restrict columns fetched to the ones given. Takes effect on next seek().
//   *
//   * @param columns Columns to fetch. Null or empty collection for all columns.
//   * @throws IOException
//   */
//  public void setFetchColumns(Collection<IteratorSetting.Column> columns) throws IOException {
//    scanner.clearColumns();
//    if (columns != null)
//      for (IteratorSetting.Column column : columns) {
//        if (column.getColumnQualifier() == null)    // fetch all columns in this column family
//          scanner.fetchColumnFamily(column.getColumnFamily());
//        else
//          scanner.fetchColumn(column.getColumnFamily(), column.getColumnQualifier());
//      }
//  }


  @Override
  public boolean hasTop() {
    return remoteIterator.hasNext();
  }

  @Override
  public void next() throws IOException {
    if (rowRangeIterator == null || remoteIterator == null)
      throw new IllegalStateException("next() called before seek() b/c rowRangeIterator or remoteIterator not set");
    remoteIterator.next(); // does nothing if there is no next (i.e. hasTop()==false)
    while (!remoteIterator.hasNext() && rowRangeIterator.hasNext()) {
      Range range = rowRangeIterator.next();
      range = range.clip(seekRange, true); // clip to the seek range
      if (range == null) // empty intersection - no more ranges by design
        return;
      scanner.setRange(range);
      remoteIterator = new PeekingIterator1<>(scanner.iterator());
    }
    // either no ranges left and we finished the current scan OR remoteIterator.hasNext()==true
//    if (hasTop())
//      log.trace(tableName + " prepared next entry " + getTopKey() + " ==> "
//          + (doWholeRow ? WholeRowIterator.decodeRow(getTopKey(), getTopValue()) : getTopValue()));
//    else
//      log.trace(tableName + " hasTop() == false");
  }

  @Override
  public Key getTopKey() {
    return remoteIterator.peek().getKey(); // returns null if hasTop()==false
  }

  @Override
  public Value getTopValue() {
    return remoteIterator.peek().getValue();
  }

  @Override
  public RemoteSourceIterator deepCopy(IteratorEnvironment iteratorEnvironment) {
    RemoteSourceIterator copy = new RemoteSourceIterator();
    try {
      copy.init(null, origOptions, iteratorEnvironment);
    } catch (IOException e) {
      log.error("Problem creating deepCopy of RemoteSourceIterator on table "+tableName, e);
      throw new RuntimeException(e);
    }
    return copy;
  }
}
