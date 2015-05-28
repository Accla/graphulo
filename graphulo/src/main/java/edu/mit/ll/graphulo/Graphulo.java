package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.*;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Holds a {@link org.apache.accumulo.core.client.Connector} to an Accumulo instance for calling core client Graphulo operations.
 */
public class Graphulo {
  private static final Logger log = LogManager.getLogger(Graphulo.class);

  private Connector connector;
  private PasswordToken password;

  public Graphulo(Connector connector, PasswordToken password) {
    this.connector = connector;
    this.password = password;
    checkCredentials();
  }

  /**
   * Check password works for this user.
   */
  private void checkCredentials() {
    try {
      if (!connector.securityOperations().authenticateUser(connector.whoami(), password))
        throw new IllegalArgumentException("instance " + connector.getInstance().getInstanceName() + ": bad username " + connector.whoami() + " with password " + new String(password.getPassword()));
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new IllegalArgumentException("instance " + connector.getInstance().getInstanceName() + ": error with username " + connector.whoami() + " with password " + new String(password.getPassword()), e);
    }
  }

  private static final Text EMPTY_TEXT = new Text();


  public void TableMult(String ATtable, String Btable, String Ctable,
                        Class<? extends IMultiplyOp> multOp, IteratorSetting sumOp,
                        Collection<Range> rowFilter,
                        String colFilterAT, String colFilterB) {
    TableMult(ATtable, Btable, Ctable, multOp, sumOp, rowFilter, colFilterAT, colFilterB, -1, false);
  }

/**
 * C += A * B.
 * User-defined "plus" and "multiply". Requires transpose table AT instead of A.
 * If C is not given, then the scan itself returns the results of A * B.
 * After operation, flushes C and removes the "plus" combiner from C.
 *
 * @param ATtable              Name of Accumulo table holding matrix transpose(A).
 * @param Btable               Name of Accumulo table holding matrix B.
 * @param Ctable               Optional. Name of table to store result. Streams back result if null.
 * @param multOp               An operation that "multiplies" two values.
 * @param sumOp                An SKVI to apply to the result table that "sums" values.
 * @param rowFilter            Optional. Row subset of ATtable and Btable, like "a,:,b,g,c,:,"
 * @param colFilterAT          Optional. Column qualifier subset of AT, restricted to not allow ranges.
 * @param colFilterB           Optional. Column qualifier subset of B, like "a,f,b,c,"
 * @param numEntriesCheckpoint Optional. # of entries before we emit a checkpoint entry from the scan.
 * @param trace                Optional. Enable server-side tracing.
 */
public void TableMult(String ATtable, String Btable, String Ctable,
                      Class<? extends IMultiplyOp> multOp, IteratorSetting sumOp,
                      Collection<Range> rowFilter,
                      String colFilterAT, String colFilterB,
                      int numEntriesCheckpoint, boolean trace) {
    if (ATtable == null || ATtable.isEmpty())
      throw new IllegalArgumentException("Please specify table AT. Given: " + ATtable);
    if (Btable == null || Btable.isEmpty())
      throw new IllegalArgumentException("Please specify table B. Given: " + Btable);
    if (ATtable.equals(Ctable))
//      log.warn("Untested combination: ATtable=Ctable="+ATtable);
      throw new UnsupportedOperationException("nyi: ATtable=Ctable=" + ATtable);
    if (Btable.equals(Ctable))
//      log.warn("Untested combination: Btable=Ctable=" + Btable);
      throw new UnsupportedOperationException("nyi: Btable=Ctable=" + Btable);
    if (Ctable != null && Ctable.isEmpty())
      Ctable = null;

//    if (multOp == null || !multOp.equals(BigDecimalMultiply.class))
//      throw new UnsupportedOperationException("only supported multOp is BigDecimalMultiply, but given: "+multOp);
//    if (sumOp == null || !sumOp.equals(BigDecimalCombiner.BigDecimalSummingCombiner.class))
//      throw new UnsupportedOperationException("only supported sumOp is BigDecimalSummingCombiner, but given: "+multOp);

    TableOperations tops = connector.tableOperations();
    if (!tops.exists(ATtable))
      throw new IllegalArgumentException("Table AT does not exist. Given: " + ATtable);
    if (!tops.exists(Btable))
      throw new IllegalArgumentException("Table B does not exist. Given: " + Btable);

    if (Ctable != null && !tops.exists(Ctable))
      try {
        tops.create(Ctable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("error trying to create C table " + Ctable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("impossible", e);
        throw new RuntimeException(e);
      }

    String instance = connector.getInstance().getInstanceName();
    String zookeepers = connector.getInstance().getZooKeepers();
    String user = connector.whoami();

    Map<String, String> opt = new HashMap<>();
    opt.put("trace", String.valueOf(trace)); // logs timing on server
    opt.put("dot", "ROW_CARTESIAN");

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

    if (Ctable != null) {
      opt.put("C.zookeeperHost", zookeepers);
      opt.put("C.instanceName", instance);
      opt.put("C.tableName", Ctable);
      opt.put("C.username", user);
      opt.put("C.password", new String(password.getPassword()));
      opt.put("C.numEntriesCheckpoint", String.valueOf(numEntriesCheckpoint)); // TODO P1: hard-coded numEntriesCheckpoint
    }

    // attach combiner on Ctable
    if (Ctable != null)
      GraphuloUtil.addCombiner(tops, Ctable, log);

    // scan B with TableMultIterator
    BatchScanner bs;
    try {
      bs = connector.createBatchScanner(Btable, Authorizations.EMPTY, 50); // TODO P2: set number of batch scan threads
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    }


    if (rowFilter != null && !rowFilter.isEmpty()) {
      if (Ctable != null) {
        opt.put("C.rowRanges", GraphuloUtil.rangesToD4MString(rowFilter)); // translate row filter to D4M notation
        bs.setRanges(Collections.singleton(new Range()));
      } else
        bs.setRanges(rowFilter);
    } else
      bs.setRanges(Collections.singleton(new Range()));

    // TODO P2: Assign priority and name dynamically, checking for conflicts.
    IteratorSetting itset = new IteratorSetting(2, TableMultIterator.class, opt);
    bs.addScanIterator(itset);

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
        System.out.println(entry.getKey() + " -> " + numEntries + c + str);
      } else {
        System.out.println(entry.getKey() + " -> " + entry.getValue());
      }
    }
    bs.close();

//    // flush
//    if (Ctable != null) {
//      try {
//        long st = System.currentTimeMillis();
//        tops.flush(Ctable, null, null, true);
//        System.out.println("flush " + Ctable + " time: " + (System.currentTimeMillis() - st) + " ms");
//      } catch (TableNotFoundException e) {
//        log.error("impossible", e);
//        throw new RuntimeException(e);
//      } catch (AccumuloSecurityException | AccumuloException e) {
//        log.error("error while flushing " + Ctable);
//        throw new RuntimeException(e);
//      }
//      GraphuloUtil.removeCombiner(tops, Ctable, log);
//    }

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

  public void addAllSumCombiner(String table) {
    GraphuloUtil.addCombiner(connector.tableOperations(), table, log);
  }

  public void Compact(String table) {
    System.out.println("Compacting " + table + "...");
    try {
      connector.tableOperations().compact(table, null, null, true, true);
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.error("error trying to compact " + table, e);
    } catch (TableNotFoundException e) {
      log.error("", e);
    }
  }


  public String AdjBFS(String Atable, String v0, int k, String Rtable, String RtableTranspose,
                       String ADegtable, String degColumn, boolean degInColQ, int minDegree, int maxDegree) {
    return AdjBFS(Atable, v0, k, Rtable, RtableTranspose, ADegtable, degColumn, degInColQ, minDegree, maxDegree, false);
  }

  @SuppressWarnings("unchecked")
  public String AdjBFS(String Atable, String v0, int k, String Rtable, String RtableTranspose,
                       String ADegtable, String degColumn, boolean degInColQ, int minDegree, int maxDegree,
                       boolean trace) {
    if (Atable == null || Atable.isEmpty())
      throw new IllegalArgumentException("Please specify Adjacency table. Given: " + Atable);
    if (ADegtable == null || ADegtable.isEmpty())
      throw new IllegalArgumentException("We currently require the use of an out-degree table for the adjacency matrix. Given: " + Atable);
    if (Rtable != null && Rtable.isEmpty())
      Rtable = null;
    if (RtableTranspose != null && RtableTranspose.isEmpty())
      RtableTranspose = null;
    if (minDegree < 1)
      minDegree = 1;
    if (maxDegree < minDegree)
      throw new IllegalArgumentException("maxDegree=" + maxDegree + " should be >= minDegree=" + minDegree);
    Text degColumnText = null;
    if (degColumn != null && !degColumn.isEmpty()) {
      degColumnText = new Text(degColumn);
      if (degInColQ)
        throw new IllegalArgumentException("not allowed: degColumn != null && degInColQ==true");
    }
    if (v0 == null)
      throw new IllegalArgumentException("null v0");
    Collection<Text> vktexts = GraphuloUtil.d4mRowToTexts(v0);

    TableOperations tops = connector.tableOperations();
    if (!tops.exists(Atable))
      throw new IllegalArgumentException("Table A does not exist. Given: " + Atable);
    if (!tops.exists(ADegtable))
      throw new IllegalArgumentException("Table ADeg does not exist. Given: " + Atable);
    if (Rtable != null && !tops.exists(Rtable))
      try {
        tops.create(Rtable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("error trying to create R table " + Rtable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("impossible", e);
        throw new RuntimeException(e);
      }
    if (RtableTranspose != null && !tops.exists(RtableTranspose))
      try {
        tops.create(RtableTranspose);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("error trying to create R table transpose" + RtableTranspose, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("impossible", e);
        throw new RuntimeException(e);
      }

    Map<String, String> opt = new HashMap<>();
    opt.put("trace", String.valueOf(trace)); // logs timing on server
    opt.put("gatherColQs", "true");
    if (Rtable != null || RtableTranspose != null) {
      String instance = connector.getInstance().getInstanceName();
      String zookeepers = connector.getInstance().getZooKeepers();
      String user = connector.whoami();
      opt.put("zookeeperHost", zookeepers);
      opt.put("instanceName", instance);
      if (Rtable != null)
        opt.put("tableName", Rtable);
      if (RtableTranspose != null)
        opt.put("tableNameTranspose", RtableTranspose);
      opt.put("username", user);
      opt.put("password", new String(password.getPassword()));

      if (Rtable != null)
        GraphuloUtil.addCombiner(tops, Rtable, log);
      if (RtableTranspose != null)
        GraphuloUtil.addCombiner(tops, RtableTranspose, log);
    }
    BatchScanner bs;
    try {
      bs = connector.createBatchScanner(Atable, Authorizations.EMPTY, 2); // TODO P2: set number of batch scan threads
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    }


    long degTime = 0, scanTime = 0;
    for (int thisk = 1; thisk <= k; thisk++) {
      if (trace)
        System.out.println("k=" + thisk + " before filter" +
          (vktexts.size() > 5 ? " #=" + String.valueOf(vktexts.size()) : ": " + vktexts.toString()));
      long t1 = System.currentTimeMillis(), dur;
      vktexts = filterTextsDegreeTable(ADegtable, degColumnText, degInColQ, minDegree, maxDegree, vktexts);
      dur = System.currentTimeMillis() - t1;
      degTime += dur;
      if (trace)
        System.out.println("Degree Lookup Time: " + dur + " ms");
      if (trace)
        System.out.println("k=" + thisk + " after  filter" +
          (vktexts.size() > 5 ? " #=" + String.valueOf(vktexts.size()) : ": " + vktexts.toString()));
      if (vktexts.isEmpty())
        break;

//      bs.setRanges(GraphuloUtil.textsToRanges(vktexts));
      bs.setRanges(Collections.singleton(new Range()));
      opt.put("rowRanges", GraphuloUtil.textsToD4mString(vktexts, v0.isEmpty() ? '\n' : v0.charAt(v0.length() - 1)));
      bs.clearScanIterators();
      IteratorSetting itset = new IteratorSetting(4, RemoteWriteIterator.class, opt);
      bs.addScanIterator(itset);

      Collection<Text> uktexts = new HashSet<>();
      long t2 = System.currentTimeMillis();
      for (Map.Entry<Key, Value> entry : bs) {
//        System.out.println("A Entry: "+entry.getKey() + " -> " + entry.getValue());
        for (String uk : (HashSet<String>) SerializationUtils.deserialize(entry.getValue().get())) {
          uktexts.add(new Text(uk));
        }

      }
      dur = System.currentTimeMillis() - t2;
      scanTime += dur;
      if (trace)
        System.out.println("BatchScan/Iterator Time: " + dur + " ms");
      vktexts = uktexts;
    }
    if (trace)
      System.out.println("Total Degree Lookup Time: " + degTime + " ms");
    if (trace)
      System.out.println("Total BatchScan/Iterator Time: " + scanTime + " ms");

    bs.close();
    return GraphuloUtil.textsToD4mString(vktexts, v0.isEmpty() ? ',' : v0.charAt(v0.length() - 1));
  }

  /**
   * Modifies texts in place, removing the entries that are out of range.
   * Assumes degrees are in the column qualifier.
   * Todo: Add a local cache parameter for known good nodes and known bad nodes,
   * so that we don't have to look them up.
   *
   * @param degColQ   Name of the degree column qualifier. Blank/null means fetch all columns, and disqualify node if any have bad degree.
   * @param degInColQ False means degree in value. True means degree in column qualifier (cannot use degColQ in this case).
   * @return The same texts object.
   */
  private Collection<Text> filterTextsDegreeTable(String ADegtable, Text degColQ, boolean degInColQ,
                                                  int minDegree, int maxDegree,
                                                  Collection<Text> texts) {
    if (degColQ != null && degColQ.getLength() == 0)
      degColQ = null;
    TableOperations tops = connector.tableOperations();
    assert ADegtable != null && !ADegtable.isEmpty() && minDegree > 0 && maxDegree >= minDegree
        && texts != null && tops.exists(ADegtable) && !(degColQ != null && degInColQ);
    if (texts.isEmpty())
      return texts;
    BatchScanner bs;
    try {
      bs = connector.createBatchScanner(ADegtable, Authorizations.EMPTY, 2); // TODO P2: set number of batch scan threads
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    }
    bs.setRanges(GraphuloUtil.textsToRanges(texts));
    if (degColQ != null)
      bs.fetchColumn(EMPTY_TEXT, degColQ);
    Text badRow = new Text();
    for (Map.Entry<Key, Value> entry : bs) {
      boolean bad = false;
//      log.debug("Deg Entry: " + entry.getKey() + " -> " + entry.getValue());
      try {
        long deg = LongCombiner.STRING_ENCODER.decode(
            degInColQ ? entry.getKey().getColumnQualifierData().getBackingArray()
                : entry.getValue().get()
        );
        if (deg < minDegree || deg > maxDegree)
          bad = true;
      } catch (ValueFormatException e) {
        log.warn("Trouble parsing degree entry as long; assuming bad degree: " + entry.getKey() + " -> " + entry.getValue(), e);
        bad = true;
      }
      if (bad) {
        boolean remove = texts.remove(entry.getKey().getRow(badRow));
        if (!remove)
          log.warn("Unrecognized entry with bad degree; cannot remove: " + entry.getKey() + " -> " + entry.getValue());
      }
    }
    bs.close();
    return texts;
  }

  /**
   * Out-degree-filtered Breadth First Search on Incidence table.
   * Conceptually k iterations of: v0 ==startPrefix==> edge ==endPrefix==> v1.
   *
   * @param Etable         Incidence table; rows are edges, column qualifiers are nodes.
   * @param v0             Starting vertices, like "v0,v5,v6,"
   * @param k              # of hops.
   * @param startPrefix    Prefix of 'start' of an edge including separator, e.g. 'out|'
   * @param endPrefix      Prefix of 'end' of an edge including separator, e.g. 'in|'
   * @param minDegree      Optional. Minimum out-degree.
   * @param maxDegree      Optional. Maximum out-degree.
   * @param outputNormal   Create E of subgraph for each of the k hops.
   * @param outputTranpose Create ET of subgraph for each of the k hops.
   */
  void EdgeBFS(String Etable, String v0, int k, String startPrefix, String endPrefix, int minDegree, int maxDegree, boolean outputNormal, boolean outputTranpose) {

  }


  void SingleTableBFS(String Stable, String v0, int k, int minDegree, int maxDegree, boolean outputNormal, boolean outputTranspose) {

  }


  /**
   * Usage with Matlab D4M:
   * <pre>
   * desiredNumTablets = ...;
   * numEntries = nnz(T);
   * G = DBaddJavaOps('edu.mit.ll.graphulo.MatlabGraphulo','instance','localhost:2181','root','secret');
   * splitPoints = G.findEvenSplits(getName(T), desiredNumTablets-1, numEntries / desiredNumTablets);
   * putSplits(T, splitPoints);
   * % Verify:
   * [splits,entriesPerSplit] = getSplits(T);
   * </pre>
   * @param numSplitPoints # of desired tablets = numSplitPoints+1
   * @param numEntriesPerTablet desired #entries per tablet = (total #entries in table) / (#desired tablets)
   * @return String with the split points with a newline separator, e.g. "ca\nf\nq\n"
   */
  public String findEvenSplits(String table, int numSplitPoints, int numEntriesPerTablet) {
    if (numSplitPoints < 0)
      throw new IllegalArgumentException("numSplitPoints: "+numSplitPoints);
    if (numSplitPoints == 0)
      return "";

    Scanner scan;
    try {
      scan = connector.createScanner(table, Authorizations.EMPTY);
    } catch (TableNotFoundException e) {
      log.error("Table does not exist: " + table, e);
      throw new RuntimeException(e);
    }
    char sep = '\n';
    StringBuilder sb = new StringBuilder();
    Iterator<Map.Entry<Key, Value>> iterator = scan.iterator();
    for (int sp = 0; sp < numSplitPoints; sp++) {
      for (int entnum = 0; entnum < numEntriesPerTablet - 1; entnum++) {
        if (!iterator.hasNext())
          throw new RuntimeException("not enough entries in table to split into " + (numSplitPoints + 1) + " tablets. Stopped after " + sp + " split points and " + entnum + " entries in the last split point");
        iterator.next();
      }
      if (!iterator.hasNext())
        throw new RuntimeException("not enough entries in table to split into " + (numSplitPoints + 1) + " tablets. Stopped after " + sp + " split points and " + (numEntriesPerTablet - 1) + " entries in the last split point");
      sb.append(iterator.next().getKey().getRow().toString())
          .append(sep);
    }
    return sb.toString();
  }

  public long countPartialProductsTableMult(String ATtable, String Btable,
                                            boolean trace) {
    if (ATtable == null || ATtable.isEmpty())
      throw new IllegalArgumentException("Please specify table AT. Given: " + ATtable);
    if (Btable == null || Btable.isEmpty())
      throw new IllegalArgumentException("Please specify table B. Given: " + Btable);

//    if (multOp == null || !multOp.equals(BigDecimalMultiply.class))
//      throw new UnsupportedOperationException("only supported multOp is BigDecimalMultiply, but given: "+multOp);
//    if (sumOp == null || !sumOp.equals(BigDecimalCombiner.BigDecimalSummingCombiner.class))
//      throw new UnsupportedOperationException("only supported sumOp is BigDecimalSummingCombiner, but given: "+multOp);

    TableOperations tops = connector.tableOperations();
    if (!tops.exists(ATtable))
      throw new IllegalArgumentException("Table AT does not exist. Given: " + ATtable);
    if (!tops.exists(Btable))
      throw new IllegalArgumentException("Table B does not exist. Given: " + Btable);


    String instance = connector.getInstance().getInstanceName();
    String zookeepers = connector.getInstance().getZooKeepers();
    String user = connector.whoami();

    Map<String, String> opt = new HashMap<>();
    opt.put("trace", String.valueOf(trace)); // logs timing on server
    opt.put("dot", "ROW_CARTESIAN");

    opt.put("AT.zookeeperHost", zookeepers);
    opt.put("AT.instanceName", instance);
    opt.put("AT.tableName", ATtable);
    opt.put("AT.username", user);
    opt.put("AT.password", new String(password.getPassword()));


    // scan B with TableMultIterator
    BatchScanner bs;
    try {
      bs = connector.createBatchScanner(Btable, Authorizations.EMPTY, 50); // TODO P2: set number of batch scan threads
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    }

    bs.setRanges(Collections.singleton(new Range()));

    // TODO P2: Assign priority and name dynamically, checking for conflicts.
    IteratorSetting itset = new IteratorSetting(2, TableMultIterator.class, opt);
    bs.addScanIterator(itset);
    itset = new IteratorSetting(3, CountAllIterator.class);
    bs.addScanIterator(itset);

    long cnt = 0;
    for (Map.Entry<Key, Value> entry : bs) {
      cnt += Long.valueOf(new String(entry.getValue().get()));
//      System.out.println("received: "+Long.valueOf(new String(entry.getValue().get())));
    }
    bs.close();
    return cnt;
  }

}
