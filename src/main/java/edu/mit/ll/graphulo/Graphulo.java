package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.*;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
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

  public static final IteratorSetting DEFAULT_PLUS_ITERATOR;

  static {
    IteratorSetting sumSetting = new IteratorSetting(6, SummingCombiner.class);
    LongCombiner.setEncodingType(sumSetting, LongCombiner.Type.STRING);
    Combiner.setCombineAllColumns(sumSetting, true);
    DEFAULT_PLUS_ITERATOR = sumSetting;
  }

  protected Connector connector;
  protected PasswordToken password;

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


  public void TableMult(String ATtable, String Btable, String Ctable, String CTtable,
                        Class<? extends IMultiplyOp> multOp, IteratorSetting plusOp,
                        Collection<Range> rowFilter,
                        String colFilterAT, String colFilterB) {
    TableMult(ATtable, Btable, Ctable, CTtable, multOp, plusOp, rowFilter, colFilterAT, colFilterB, -1, false);
  }

  public void SpEWiseX(String Atable, String Btable, String Ctable, String CTtable,
                       Class<? extends IMultiplyOp> multOp, IteratorSetting plusOp,
                       Collection<Range> rowFilter,
                       String colFilterAT, String colFilterB,
                       int numEntriesCheckpoint, boolean trace) {
    TwoTable(Atable, Btable, Ctable, CTtable, TwoTableIterator.DOT_TYPE.ROW_COLF_COLQ_MATCH,
            multOp, plusOp, rowFilter, colFilterAT, colFilterB, numEntriesCheckpoint, trace);
  }

  /**
   * C += A * B.
   * User-defined "plus" and "multiply". Requires transpose table AT instead of A.
   * If C is not given, then the scan itself returns the results of A * B. (not thoroughly tested)
   *
   * @param ATtable              Name of Accumulo table holding matrix transpose(A).
   * @param Btable               Name of Accumulo table holding matrix B.
   * @param Ctable               Name of table to store result. Null means don't store the result.
   * @param CTtable              Name of table to store transpose of result. Null means don't store the transpose.
   * @param multOp               An operation that "multiplies" two values.
   * @param plusOp               An SKVI to apply to the result table that "sums" values. Not applied if null.
   * @param rowFilter            Row subset of ATtable and Btable, like "a,:,b,g,c,:,". Null means run on all rows.
   * @param colFilterAT          Column qualifier subset of AT, restricted to not allow ranges. Null means run on all columns.
   * @param colFilterB           Column qualifier subset of B, like "a,f,b,c,". Null means run on all columns.
   * @param numEntriesCheckpoint # of entries before we emit a checkpoint entry from the scan. -1 means no monitoring.
   * @param trace                Enable server-side performance tracing.
   */
  public void TableMult(String ATtable, String Btable, String Ctable, String CTtable,
                        Class<? extends IMultiplyOp> multOp, IteratorSetting plusOp,
                        Collection<Range> rowFilter,
                        String colFilterAT, String colFilterB,
                        int numEntriesCheckpoint, boolean trace) {
    TwoTable(ATtable, Btable, Ctable, CTtable, TwoTableIterator.DOT_TYPE.ROW_CARTESIAN,
            multOp, plusOp, rowFilter, colFilterAT, colFilterB, numEntriesCheckpoint, trace);
  }

  public void TwoTable(String ATtable, String Btable, String Ctable, String CTtable, TwoTableIterator.DOT_TYPE dot,
                        Class<? extends IMultiplyOp> multOp, IteratorSetting plusOp,
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
    if (ATtable.equals(CTtable))
      throw new UnsupportedOperationException("nyi: ATtable=CTtable=" + ATtable);
    if (Btable.equals(CTtable))
      throw new UnsupportedOperationException("nyi: Btable=CTtable=" + Btable);

    if (Ctable != null && Ctable.isEmpty())
      Ctable = null;
    if (CTtable != null && CTtable.isEmpty())
      CTtable = null;
    if (Ctable == null && CTtable == null)
      log.warn("Streaming back result of multiplication to client does not guarantee correctness." +
          "In particular, if Accumulo destroys, re-inits and re-seeks an iterator stack, the stack may not recover.");

    if (multOp == null)
      throw new IllegalArgumentException("multOp is required but given null");

    TableOperations tops = connector.tableOperations();
    if (!tops.exists(ATtable))
      throw new IllegalArgumentException("Table AT does not exist. Given: " + ATtable);
    if (!tops.exists(Btable))
      throw new IllegalArgumentException("Table B does not exist. Given: " + Btable);

    if (Ctable != null && !tops.exists(Ctable))
      try {
        tops.create(Ctable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("error trying to create Ctable " + Ctable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }

    if (CTtable != null && !tops.exists(CTtable))
      try {
        tops.create(CTtable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("error trying to create CTtable " + Ctable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }

    String instance = connector.getInstance().getInstanceName();
    String zookeepers = connector.getInstance().getZooKeepers();
    String user = connector.whoami();

    Map<String, String> opt = new HashMap<>();
    opt.put("trace", String.valueOf(trace)); // logs timing on server
    opt.put("dot", dot.name());
    opt.put("multiplyOp", multOp.getName());

    opt.put("AT.zookeeperHost", zookeepers);
    opt.put("AT.instanceName", instance);
    opt.put("AT.tableName", ATtable);
    opt.put("AT.username", user);
    opt.put("AT.password", new String(password.getPassword()));
    if (colFilterAT != null)
      opt.put("AT.colFilter", colFilterAT);

    if (Ctable != null || CTtable != null) {
      opt.put("C.zookeeperHost", zookeepers);
      opt.put("C.instanceName", instance);
      if (Ctable != null)
        opt.put("C.tableName", Ctable);
      if (CTtable != null)
        opt.put("C.tableNameTranspose", CTtable);
      opt.put("C.username", user);
      opt.put("C.password", new String(password.getPassword()));
      opt.put("C.numEntriesCheckpoint", String.valueOf(numEntriesCheckpoint));
    }

    if (Ctable != null && plusOp != null)
      GraphuloUtil.applyIteratorSoft(plusOp, tops, Ctable);
    if (CTtable != null && plusOp != null)
      GraphuloUtil.applyIteratorSoft(plusOp, tops, CTtable);

    // scan B with TableMultIterator
    BatchScanner bs;
    try {
      bs = connector.createBatchScanner(Btable, Authorizations.EMPTY, 50); // TODO P2: set number of batch scan threads
    } catch (TableNotFoundException e) {
      log.error("crazy", e);
      throw new RuntimeException(e);
    }

    if (rowFilter != null && !rowFilter.isEmpty()) {
      if (Ctable != null || CTtable != null) {
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

    // for monitoring: reduce table-level parameter controlling when Accumulo sends back an entry to the client
    String prevPropTableScanMaxMem = null;
    if (numEntriesCheckpoint > 0 && (Ctable != null || CTtable != null))
      try {
        for (Map.Entry<String, String> entry : tops.getProperties(Btable)) {
          if (entry.getKey().equals("table.scan.max.memory"))
            prevPropTableScanMaxMem = entry.getValue();
        }
//        System.out.println("prevPropTableScanMaxMem: "+prevPropTableScanMaxMem);
        if (prevPropTableScanMaxMem == null)
          log.warn("expected table.scan.max.memory to be set on table " + Btable);
        else {
          tops.setProperty(Btable, "table.scan.max.memory", "1B");
        }
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.warn("error trying to get and set property table.scan.max.memory for " + Btable, e);
      } catch (TableNotFoundException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }

    // Do the BatchScan on B
    try {
      for (Map.Entry<Key, Value> entry : bs) {
        if (Ctable != null || CTtable != null) {
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
    } finally {
      bs.close();
      if (prevPropTableScanMaxMem != null) {
        try {
          tops.setProperty(Btable, "table.scan.max.memory", prevPropTableScanMaxMem);
        } catch (AccumuloException | AccumuloSecurityException e) {
          log.error("cannot reset table.scan.max.memory property for " + Btable + " to " + prevPropTableScanMaxMem, e);
        }
      }
    }


//    // flush
//    if (Ctable != null) {
//      try {
//        long st = System.currentTimeMillis();
//        tops.flush(Ctable, null, null, true);
//        System.out.println("flush " + Ctable + " time: " + (System.currentTimeMillis() - st) + " ms");
//      } catch (TableNotFoundException e) {
//        log.error("crazy", e);
//        throw new RuntimeException(e);
//      } catch (AccumuloSecurityException | AccumuloException e) {
//        log.error("error while flushing " + Ctable);
//        throw new RuntimeException(e);
//      }
//      GraphuloUtil.removeCombiner(tops, Ctable, log);
//    }

  }


  /**
   * Use LongCombiner to sum.
   */

  public String AdjBFS(String Atable, String v0, int k, String Rtable, String RtableTranspose,
                       String ADegtable, String degColumn, boolean degInColQ, int minDegree, int maxDegree) {

    return AdjBFS(Atable, v0, k, Rtable, RtableTranspose,
        ADegtable, degColumn, degInColQ, minDegree, maxDegree,
        DEFAULT_PLUS_ITERATOR, false);
  }

  /**
   * Adjacency table Breadth First Search. Sums entries into Rtable from each step of the BFS.
   *
   * @param Atable      Name of Accumulo table holding matrix transpose(A).
   * @param v0          Starting nodes, like "a,f,b,c,". Null or empty string "" means start from all nodes.
   * @param k           Number of steps
   * @param Rtable      Name of table to store result. Null means don't store the result.
   * @param RTtable     Name of table to store transpose of result. Null means don't store the transpose.
   * @param ADegtable   Name of table holding out-degrees for A. Leave null to filter on the fly with
   *                    the {@link edu.mit.ll.graphulo.SmallLargeRowFilter}, or do no filtering if minDegree=0 and maxDegree=Integer.MAX_VALUE.
   * @param degColumn   Name of column for out-degrees in ADegtable. Leave null if degInColQ==true.
   * @param degInColQ   True means degree is in the Column Qualifier. False means degree is in the Value.
   * @param minDegree   Minimum out-degree. Checked before doing any searching, at every step, from ADegtable. Pass 0 for no filtering.
   * @param maxDegree   Maximum out-degree. Checked before doing any searching, at every step, from ADegtable. Pass Integer.MAX_VALUE for no filtering.
   * @param plusOp      An SKVI to apply to the result table that "sums" values. Not applied if null.
   * @param trace       Enable server-side performance tracing.
   * @return          The nodes reachable in exactly k steps from v0.
   */
  @SuppressWarnings("unchecked")
  public String AdjBFS(String Atable, String v0, int k, String Rtable, String RTtable,
                       String ADegtable, String degColumn, boolean degInColQ, int minDegree, int maxDegree,
                       IteratorSetting plusOp,
                       boolean trace) {
    boolean needDegreeFiltering = minDegree > 0 || maxDegree < Integer.MAX_VALUE;
    if (Atable == null || Atable.isEmpty())
      throw new IllegalArgumentException("Please specify Adjacency table. Given: " + Atable);
    if (ADegtable != null && ADegtable.isEmpty())
      ADegtable = null;
    if (Rtable != null && Rtable.isEmpty())
      Rtable = null;
    if (RTtable != null && RTtable.isEmpty())
      RTtable = null;
    if (minDegree < 1)
      minDegree = 1;
    if (maxDegree < minDegree)
      throw new IllegalArgumentException("maxDegree=" + maxDegree + " should be >= minDegree=" + minDegree);
    Text degColumnText = null;
    if (needDegreeFiltering && degColumn != null && !degColumn.isEmpty()) {
      degColumnText = new Text(degColumn);
      if (degInColQ)
        throw new IllegalArgumentException("not allowed: degColumn != null && degInColQ==true");
    }
    if (plusOp != null && plusOp.getPriority() >= 20)
      log.warn("Sum iterator setting is >=20. Are you sure you want the priority after the default Versioning iterator priority? " + plusOp);
    if (v0 != null && v0.isEmpty())
      v0 = null;
    Collection<Text> vktexts = v0 == null ? null : GraphuloUtil.d4mRowToTexts(v0);

    TableOperations tops = connector.tableOperations();
    if (!tops.exists(Atable))
      throw new IllegalArgumentException("Table A does not exist. Given: " + Atable);
    if (Rtable != null && !tops.exists(Rtable))
      try {
        tops.create(Rtable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("error trying to create R table " + Rtable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }
    if (RTtable != null && !tops.exists(RTtable))
      try {
        tops.create(RTtable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("error trying to create R table transpose" + RTtable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }

    Map<String, String> opt = new HashMap<>();
    opt.put("trace", String.valueOf(trace)); // logs timing on server
    opt.put("gatherColQs", "true");
    if (Rtable != null || RTtable != null) {
      String instance = connector.getInstance().getInstanceName();
      String zookeepers = connector.getInstance().getZooKeepers();
      String user = connector.whoami();
      opt.put("zookeeperHost", zookeepers);
      opt.put("instanceName", instance);
      if (Rtable != null)
        opt.put("tableName", Rtable);
      if (RTtable != null)
        opt.put("tableNameTranspose", RTtable);
      opt.put("username", user);
      opt.put("password", new String(password.getPassword()));

      if (Rtable != null && plusOp != null)
        GraphuloUtil.applyIteratorSoft(plusOp, tops, Rtable);
      if (RTtable != null && plusOp != null)
        GraphuloUtil.applyIteratorSoft(plusOp, tops, RTtable);
    }
    BatchScanner bs;
    try {
      bs = connector.createBatchScanner(Atable, Authorizations.EMPTY, 2); // TODO P2: set number of batch scan threads
    } catch (TableNotFoundException e) {
      log.error("crazy", e);
      throw new RuntimeException(e);
    }

    IteratorSetting itsetDegreeFilter = null;
    if (needDegreeFiltering && ADegtable == null) {
      itsetDegreeFilter = new IteratorSetting(3, SmallLargeRowFilter.class);
      SmallLargeRowFilter.setMinColumns(itsetDegreeFilter, minDegree);
      SmallLargeRowFilter.setMaxColumns(itsetDegreeFilter, maxDegree);
    }


    long degTime = 0, scanTime = 0;
    for (int thisk = 1; thisk <= k; thisk++) {
      if (trace)
        if (vktexts != null)
          System.out.println("k=" + thisk + " before filter" +
            (vktexts.size() > 5 ? " #=" + String.valueOf(vktexts.size()) : ": " + vktexts.toString()));
        else
          System.out.println("k=" + thisk + " ALL nodes");
      long t1 = System.currentTimeMillis(), dur;

      vktexts = needDegreeFiltering && ADegtable != null
              ? filterTextsDegreeTable(ADegtable, degColumnText, degInColQ, minDegree, maxDegree, vktexts)
              : vktexts;
      dur = System.currentTimeMillis() - t1;
      degTime += dur;
      if (trace)
        System.out.println("Degree Lookup Time: " + dur + " ms");
      if (trace && vktexts != null)
        System.out.println("k=" + thisk + " after  filter" +
            (vktexts.size() > 5 ? " #=" + String.valueOf(vktexts.size()) : ": " + vktexts.toString()));
      if (vktexts != null && vktexts.isEmpty())
        break;

//      bs.setRanges(GraphuloUtil.textsToRanges(vktexts));
      bs.setRanges(Collections.singleton(new Range()));
      if (vktexts != null)
        opt.put("rowRanges", GraphuloUtil.textsToD4mString(vktexts, v0.isEmpty() ? '\n' : v0.charAt(v0.length() - 1)));
      bs.clearScanIterators();
      if (needDegreeFiltering && ADegtable == null)
        bs.addScanIterator(itsetDegreeFilter);
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
    return GraphuloUtil.textsToD4mString(vktexts, v0 == null ? ',' : v0.charAt(v0.length() - 1));
  }

  /**
   * Modifies texts in place, removing the entries that are out of range.
   * Does nothing if texts is null or the empty collection.
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
    if (texts == null || texts.isEmpty())
      return texts;
    if (degColQ != null && degColQ.getLength() == 0)
      degColQ = null;
    TableOperations tops = connector.tableOperations();
    assert ADegtable != null && !ADegtable.isEmpty() && minDegree > 0 && maxDegree >= minDegree
        && tops.exists(ADegtable) && !(degColQ != null && degInColQ);
    BatchScanner bs;
    try {
      bs = connector.createBatchScanner(ADegtable, Authorizations.EMPTY, 2); // TODO P2: set number of batch scan threads
    } catch (TableNotFoundException e) {
      log.error("crazy", e);
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
   *
   * @param numSplitPoints      # of desired tablets = numSplitPoints+1
   * @param numEntriesPerTablet desired #entries per tablet = (total #entries in table) / (#desired tablets)
   * @return String with the split points with a newline separator, e.g. "ca\nf\nq\n"
   */
  public String findEvenSplits(String table, int numSplitPoints, int numEntriesPerTablet) {
    if (numSplitPoints < 0)
      throw new IllegalArgumentException("numSplitPoints: " + numSplitPoints);
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
      log.error("crazy", e);
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
