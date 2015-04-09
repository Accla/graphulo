package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.PeekingIterator;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * SKVI that writes to an Accumulo table.
 * Does all work in seek() method. hasTop() is always false.
 */
public class RemoteWriteIterator extends WrappingIterator implements OptionDescriber {
    private static final Logger log = LogManager.getLogger(RemoteWriteIterator.class);

    private String instanceName;
    private String tableName;
    private String zookeeperHost;
    private String username;
    private AuthenticationToken auth;
    /** Zookeeper timeout in milliseconds */
    private int timeout = -1;

    /** Created in init().  */
    private BatchWriter writer;

    /** Call init() after construction. */
    public RemoteWriteIterator() {}

    /** Copies configuration from other, including connector,
     * EXCEPT creates a new, separate scanner.
     * No need to call init(). */
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
        Map<String,String> optDesc = new LinkedHashMap<>();
        optDesc.put("zookeeperHost", "address and port");
        optDesc.put("timeout", "Zookeeper timeout between 1000 and 300000 (default 1000)");
        optDesc.put("instanceName", "");
        optDesc.put("tableName", "");
        optDesc.put("username", "");
        optDesc.put("password", "(Anyone who can read the Accumulo table config OR the log files will see your password in plaintext.)");
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

    public static boolean validateOptionsStatic(Map<String,String> options) {
        // Shadow all the fields =)
        String zookeeperHost=null, instanceName=null, tableName=null, username=null;
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
                            throw new IllegalArgumentException("timeout out of range [1000,300000]: "+t);
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
                default:
                    throw new IllegalArgumentException("unknown option: "+entry);
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
                default:
                    log.warn("Unrecognized option: " + entry);
                    continue;
            }
            log.trace("Option OK: "+entry);
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
        super.init(source, map, iteratorEnvironment); // sets source
        if (source == null)
            throw new IllegalArgumentException("source must be specified");

        parseOptions(map);

        setupConnectorWriter();

        log.debug("RemoteWriteIterator on table "+tableName+": init() succeeded");
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
            log.error("failed to connect to Accumulo instance "+instanceName,e);
            throw new RuntimeException(e);
        }

        BatchWriterConfig bwc = new BatchWriterConfig();
        // TODO: consider max memory, max latency, timeout, ... on writer

        try {
            writer = connector.createBatchWriter(tableName, bwc);
        } catch (TableNotFoundException e) {
            log.error(tableName+" does not exist in instance "+instanceName, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        log.info("finalize() called on RemoteWriteIterator for table "+tableName);
        super.finalize();
        writer.close();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        log.debug("RemoteWriteIterator on table "+tableName+": about to seek() to range "+range);
        super.seek(range, columnFamilies, inclusive);

        writeAllSourceEntries();
    }

    private void writeAllSourceEntries() throws IOException {
        Mutation m = null;
        try {
            while (super.hasTop()) {
                Key k = super.getTopKey();
                Value v = super.getTopValue();
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
                Watch.instance.start(Watch.PerfSpan.WriteGetNext);
                try {
                    super.next();
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



    // will always return false by design
//    @Override
//    public boolean hasTop() {
//        return false;
//    }

//    @Override
//    public void next() throws IOException {
//        throw new IllegalStateException("next() called before seek() b/c rowRangeIterator or remoteIterator not set");
//    }

//    @Override
//    public Key getTopKey() {
//        return null;
//    }

//    @Override
//    public Value getTopValue() {
//        return null;
//    }

    @Override
    public RemoteWriteIterator deepCopy(IteratorEnvironment iteratorEnvironment) {
        return new RemoteWriteIterator(this);
    }


}
