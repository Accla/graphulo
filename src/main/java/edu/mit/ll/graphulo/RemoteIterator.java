package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Reads a whole row from a remote Accumulo table.
 */
public class RemoteIterator implements SortedKeyValueIterator<Key,Value> {
    private static final Logger log = LogManager.getLogger(RemoteIterator.class);

    private String instanceName;
    private String tableName;
    private String zookeeperHost;
    private String username;
    private AuthenticationToken auth;
    /** Zookeeper timeout in milliseconds */
    private int timeout = -1;

    private boolean doWholeRow = false;
    private SortedSet<Range> rowRanges = new TreeSet<>(Collections.singleton(new Range()));

    /** Holds the current range we are scanning.
     * Goes through the part of ranges after seeking to the beginning of the seek() clip. */
    private Iterator<Range> rowRangeIterator;
    // Collection<Range> colFilter; todo

    /** Created in init().  */
    private Scanner scanner;
    /** Buffers one entry from the remote table. */
    private PeekingIterator<Map.Entry<Key,Value>> remoteIterator;

    /** Call init() after construction. */
    public RemoteIterator() {}

    /** Copies configuration from other, including connector,
     * EXCEPT creates a new, separate scanner.
     * No need to call init(). */
    public RemoteIterator(RemoteIterator other) {
        other.instanceName = instanceName;
        other.tableName = tableName;
        other.zookeeperHost = zookeeperHost;
        other.username = username;
        other.auth = auth;
        other.timeout = timeout;
        other.doWholeRow = doWholeRow;
        other.rowRanges = rowRanges;
        other.setupConnectorScanner();
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

                case "doWholeRow":
                    doWholeRow = Boolean.parseBoolean(entry.getValue());
                    break;
                case "rowRanges":
                    rowRanges = parseRanges(entry.getValue());
                    break;
                default:
                    log.info("Unrecognized option: "+entry);
                    continue;
            }
            log.info("Option OK: "+entry);
        }
        // Required options
        if (zookeeperHost == null ||
                instanceName == null ||
                tableName == null ||
                username == null ||
                auth == null)
            throw new IllegalArgumentException("not enough options provided");

    }

    /** Parse string s in the Matlab format "row1,row5,row7,:,row9,w,:,z,zz,:,"
     * Does not have to be ordered but cannot overlap.
     * @param s -
     * @return a bunch of ranges
     */
    private SortedSet<Range> parseRanges(String s) {
        // use Range.mergeOverlapping
        // then sort
        throw new UnsupportedOperationException("not yet implemented");
        //return null;
    }

    /**
     * Initializes the iterator. Data should not be read from the source in this method.
     *
     * @param source null
     * @param map options
     * @param iteratorEnvironment    @throws java.io.IOException
     *                               unused.
     * @throws IllegalArgumentException      if there are problems with the options.
     * @throws UnsupportedOperationException if not supported.
     */
    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> map, IteratorEnvironment iteratorEnvironment) throws IOException {
        // temp remove for testing
//        if (source != null)
//            throw new IllegalArgumentException("Does not take a parent source.");

        parseOptions(map);

        setupConnectorScanner();

        if (doWholeRow) {
            // TODO: make priority dynamic in case 25 is taken; make name dynamic in case this iterator already exists. Or buffer here.
            IteratorSetting iset = new IteratorSetting(25, WholeRowIterator.class);
            scanner.addScanIterator(iset);
        }

        log.info("init(...) succeeded; connected to table "+tableName+" of instance "+instanceName);
    }

    private void setupConnectorScanner() {
        ClientConfiguration cc = ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeeperHost);
        if (timeout == -1)
            cc = cc.withZkTimeout(timeout);
        Instance instance = new ZooKeeperInstance(cc);
        Connector connector;
        try {
            connector = instance.getConnector(username, auth);
        } catch (AccumuloException | AccumuloSecurityException e) {
            log.error("failed to connect to Accumulo instance "+instanceName,e);
            throw new RuntimeException(e);
        }

        try {
            scanner = connector.createScanner(tableName, Authorizations.EMPTY);
        } catch (TableNotFoundException e) {
            log.error(tableName+" does not exist in instance "+instanceName, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        scanner.close();

    }

    /**
     * Seeks to the first key in the Range, restricting the resulting K,V pairs to those with the specified columns. An iterator does not have to stop at the end
     * of the range. The whole range is provided so that iterators can make optimizations.
     * <p/>
     * Seek may be called multiple times with different parameters after {@link #init} is called.
     * <p/>
     * Iterators that examine groups of adjacent key/value pairs (e.g. rows) to determine their top key and value should be sure that they properly handle a seek
     * to a key in the middle of such a group (e.g. the middle of a row). Even if the client always seeks to a range containing an entire group (a,c), the tablet
     * server could send back a batch of entries corresponding to (a,b], then reseek the iterator to range (b,c) when the scan is continued.
     * <p/>
     * {@code columnFamilies} is used, at the lowest level, to determine which data blocks inside of an RFile need to be opened for this iterator. This set of data
     * blocks is also the set of locality groups defined for the given table. If no columnFamilies are provided, the data blocks for all locality groups inside of
     * the correct RFile will be opened and seeked in an attempt to find the correct start key, regardless of the startKey in the {@code range}.
     * <p/>
     * In an Accumulo instance in which multiple locality groups exist for a table, it is important to ensure that {@code columnFamilies} is properly set to the
     * minimum required column families to ensure that data from separate locality groups is not inadvertently read.
     *
     * @param range      <tt>Range</tt> of keys to iterate over.
     * @param columnFamilies unused
     * @param inclusive      <tt>boolean</tt> that indicates whether to include (true) or exclude (false) column families.
     * @throws java.io.IOException if an I/O error occurs.
     * @throws IllegalArgumentException if there are problems with the parameters.
     */
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {

        /** configure Scanner to the first entry to inject after the start of the range.
         Range comparison: infinite start first, then inclusive start, then exclusive start
         {@link org.apache.accumulo.core.data.Range#compareTo(Range)} */
        // P2: think about clipping the end if given an end key not -inf
        rowRangeIterator = rowRanges.tailSet(range).iterator();
        remoteIterator = new PeekingIterator<>(java.util.Collections.<Map.Entry<Key,Value>>emptyIterator());
        next();
    }



    /**
     * Returns true if the iterator has more elements.
     *
     * @return <tt>true</tt> if the iterator has more elements.
     * @throws IllegalStateException if called before seek.
     */
    @Override
    public boolean hasTop() {
        return remoteIterator.hasNext();
    }

    /**
     * Advances to the next K,V pair. Note that in minor compaction scope and in non-full major compaction scopes the iterator may see deletion entries. These
     * entries should be preserved by all iterators except ones that are strictly scan-time iterators that will never be configured for the minc or majc scopes.
     * Deletion entries are only removed during full major compactions.
     *
     * @throws java.io.IOException    if an I/O error occurs.
     * @throws IllegalStateException  if called before seek.
     * @throws NoSuchElementException if next element doesn't exist.
     */
    @Override
    public void next() throws IOException {
        if (rowRangeIterator == null || remoteIterator == null)
            throw new IllegalStateException("next() called before seek() b/c rowRangeIterator or remoteIterator not set");
        remoteIterator.next(); // does nothing if there is no next (i.e. hasTop()==false)
        while (!remoteIterator.hasNext() && rowRangeIterator.hasNext()) {
            Range range = rowRangeIterator.next();
            scanner.setRange(range);
            remoteIterator = new PeekingIterator<>(scanner.iterator());
        }
        // either no ranges left and we finished the current scan OR remoteIterator.hasNext()==true
    }



    /**
     * Returns top key. Can be called 0 or more times without affecting behavior of next() or hasTop(). Note that in minor compaction scope and in non-full major
     * compaction scopes the iterator may see deletion entries. These entries should be preserved by all iterators except ones that are strictly scan-time
     * iterators that will never be configured for the minc or majc scopes. Deletion entries are only removed during full major compactions.
     *
     * @return <tt>K</tt>
     * @throws IllegalStateException  if called before seek.
     * @throws NoSuchElementException if top element doesn't exist.
     */
    @Override
    public Key getTopKey() {
        return remoteIterator.peek().getKey(); // returns null if hasTop()==false
    }

    /**
     * Returns top value. Can be called 0 or more times without affecting behavior of next() or hasTop().
     *
     * @return <tt>V</tt>
     * @throws IllegalStateException  if called before seek.
     * @throws NoSuchElementException if top element doesn't exist.
     */
    @Override
    public Value getTopValue() {
        return remoteIterator.peek().getValue();
    }

    /**
     * Creates a deep copy of this iterator as though seek had not yet been called. init should be called on an iterator before deepCopy is called. init should
     * not need to be called on the copy that is returned by deepCopy; that is, when necessary init should be called in the deepCopy method on the iterator it
     * returns. The behavior is unspecified if init is called after deepCopy either on the original or the copy.
     *
     * @param iteratorEnvironment ok
     * @return <tt>SortedKeyValueIterator</tt> a copy of this iterator (with the same source and settings).
     * @throws UnsupportedOperationException if not supported.
     */
    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment iteratorEnvironment) {
        return new RemoteIterator(this);
        // I don't think we need to copy the current scan location
    }
}
