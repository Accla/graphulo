package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Graphulo operation implementation.
 */
public class Graphulo implements IGraphulo {
  private static final Logger log = LogManager.getLogger(Graphulo.class);

  private Connector connector;
  private PasswordToken password;

  public Graphulo(Connector connector, PasswordToken password) {
    this.connector = connector;
    this.password = password;
    checkCredentials();
  }

  /** Check password works for this user. */
  private void checkCredentials() {
    try {
      if (!connector.securityOperations().authenticateUser(connector.whoami(), password))
        throw new IllegalArgumentException("instance "+connector.getInstance().getInstanceName()+": bad username "+connector.whoami()+" with password "+new String(password.getPassword()));
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new IllegalArgumentException("instance "+connector.getInstance().getInstanceName()+": error with username "+connector.whoami()+" with password "+new String(password.getPassword()), e);
    }
  }

  static final Text EMPTY_TEXT = new Text();

  @Override
  public void TableMult(String ATtable, String Btable, String Ctable,
                        Class<? extends IMultiplyOp> multOp, Class<? extends Combiner> sumOp,
                        Collection<Range> rowFilter,
                        String colFilterAT, String colFilterB) {
    TableMult(ATtable, Btable, Ctable, multOp, sumOp, rowFilter, colFilterAT, colFilterB, 250000, true);
  }

  public void TableMult(String ATtable, String Btable, String Ctable,
                        Class<? extends IMultiplyOp> multOp, Class<? extends Combiner> sumOp,
                        Collection<Range> rowFilter,
                        String colFilterAT, String colFilterB,
                        int numEntriesCheckpoint, boolean trace) {
    if (ATtable == null || ATtable.isEmpty())
      throw new IllegalArgumentException("Please specify table AT. Given: "+ATtable);
    if (Btable == null || Btable.isEmpty())
      throw new IllegalArgumentException("Please specify table B. Given: "+Btable);
    if (ATtable.equals(Ctable))
//      log.warn("Untested combination: ATtable=Ctable="+ATtable);
      throw new UnsupportedOperationException("nyi: ATtable=Ctable="+ATtable);
    if (Btable.equals(Ctable))
//      log.warn("Untested combination: Btable=Ctable=" + Btable);
      throw new UnsupportedOperationException("nyi: Btable=Ctable=" + Btable);

//    if (multOp == null || !multOp.equals(BigDecimalMultiply.class))
//      throw new UnsupportedOperationException("only supported multOp is BigDecimalMultiply, but given: "+multOp);
//    if (sumOp == null || !sumOp.equals(BigDecimalCombiner.BigDecimalSummingCombiner.class))
//      throw new UnsupportedOperationException("only supported sumOp is BigDecimalSummingCombiner, but given: "+multOp);

    TableOperations tops = connector.tableOperations();
    if (!tops.exists(ATtable))
      throw new IllegalArgumentException("Table AT does not exist. Given: "+ATtable);
    if (!tops.exists(Btable))
      throw new IllegalArgumentException("Table B does not exist. Given: "+Btable);

    if (Ctable != null && !Ctable.isEmpty() && !tops.exists(Ctable))
      try {
        tops.create(Ctable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("error trying to create C table "+Ctable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("impossible",e);
        throw new RuntimeException(e);
      }

    String instance = connector.getInstance().getInstanceName();
    String zookeepers = connector.getInstance().getZooKeepers();
    String user = connector.whoami();

    Map<String,String> opt = new HashMap<>();
    opt.put("trace",String.valueOf(trace)); // logs timing on server

    opt.put("AT.zookeeperHost", zookeepers);
    opt.put("AT.instanceName", instance);
    opt.put("AT.tableName", ATtable);
    opt.put("AT.username", user);
    opt.put("AT.password", new String(password.getPassword()));
    if (colFilterAT != null)
      opt.put("AT.colFilter", colFilterAT);
//    opt.put("B.zookeeperHost", zookeepers);
//    opt.put("B.instanceName", instance);
//    opt.put("B.tableName", Btable);
//    opt.put("B.username", user);
//    opt.put("B.password", new String(password.getPassword()));
    // DH: not needed. Use fetchColumn() on bs below.
//    if (colFilterB != null)
//      opt.put("B.colFilter", colFilterB);

    if (Ctable != null && !Ctable.isEmpty()) {
      opt.put("C.zookeeperHost", zookeepers);
      opt.put("C.instanceName", instance);
      opt.put("C.tableName", Ctable);
      opt.put("C.username", user);
      opt.put("C.password", new String(password.getPassword()));
      opt.put("C.numEntriesCheckpoint", String.valueOf(numEntriesCheckpoint)); // TODO P1: hard-coded numEntriesCheckpoint
    }

    // attach combiner on Ctable
    // TODO P2: Assign priority and name dynamically, checking for conflicts.
    Map<String, String> optSum = new HashMap<>();
    optSum.put("all", "true");
    IteratorSetting iSum = new IteratorSetting(19,"plus",BigDecimalCombiner.BigDecimalSummingCombiner.class, optSum);

    // checking if iterator already exists. Not checking for conflicts.
    try {
      IteratorSetting existing;
      EnumSet<IteratorUtil.IteratorScope> enumSet = EnumSet.noneOf(IteratorUtil.IteratorScope.class);
      for (IteratorUtil.IteratorScope scope : IteratorUtil.IteratorScope.values()) {
        existing = tops.getIteratorSetting(Ctable, "plus", IteratorUtil.IteratorScope.majc);
        if (existing == null)
          enumSet.add(scope);
      }
      tops.attachIterator(Ctable, iSum, enumSet);
    } catch (AccumuloSecurityException | AccumuloException e) {
      log.error("error trying to add BigDecimalSummingCombiner to " + Ctable, e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    }

    // scan B with TableMultIterator
    BatchScanner bs;
    try {
      bs = connector.createBatchScanner(Btable, Authorizations.EMPTY, 2); // TODO P2: set number of batch scan threads
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    }
    // TODO P2: Assign priority and name dynamically, checking for conflicts.
    IteratorSetting itset = new IteratorSetting(2, TableMultIterator.class, opt);
    bs.addScanIterator(itset);

    if (rowFilter == null || rowFilter.isEmpty())
      rowFilter = Collections.singleton(new Range());
    bs.setRanges(rowFilter);

    if (colFilterB != null)
      for (Text text : GraphuloUtil.d4mRowToTexts(colFilterB)) {
        bs.fetchColumn(EMPTY_TEXT, text);
      }

    for (Map.Entry<Key, Value> entry : bs) {
      if (Ctable != null && !Ctable.isEmpty()) {
        Value v = entry.getValue();
        ByteBuffer bb = ByteBuffer.wrap(v.get());
        int numEntries = bb.getInt();
        char c = bb.getChar();
        String str = new Value(bb).toString();
        System.out.println(entry.getKey().toString() + " -> " + numEntries + c + str);
      } else {
        System.out.println(entry.getKey().toString() + " -> " + entry.getValue());
      }
    }
  }

  public void CancelCompact(String table) {
    try {
      connector.tableOperations().cancelCompaction(table);
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.error("error trying to cancel compaction for " + table, e);
    } catch (TableNotFoundException e) {
      log.error("", e);
    }
  }

}
