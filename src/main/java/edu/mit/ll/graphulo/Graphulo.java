package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.mult.EdgeBFSMultiply;
import edu.mit.ll.graphulo.mult.IMultiplyOp;
import edu.mit.ll.graphulo.reducer.EdgeBFSReducer;
import edu.mit.ll.graphulo.reducer.GatherColQReducer;
import edu.mit.ll.graphulo.reducer.SingleBFSReducer;
import edu.mit.ll.graphulo.skvi.CountAllIterator;
import edu.mit.ll.graphulo.skvi.RemoteWriteIterator;
import edu.mit.ll.graphulo.skvi.SmallLargeRowFilter;
import edu.mit.ll.graphulo.skvi.TableMultIterator;
import edu.mit.ll.graphulo.skvi.TwoTableIterator;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.accumulo.core.iterators.user.ColumnSliceFilter;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

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
            multOp, plusOp, rowFilter, colFilterAT, colFilterB,
        false, false, numEntriesCheckpoint, trace);
  }

  public void SpEWiseSum(String Atable, String Btable, String Ctable, String CTtable,
                       Class<? extends IMultiplyOp> multOp, IteratorSetting plusOp,
                       Collection<Range> rowFilter,
                       String colFilterAT, String colFilterB,
                       int numEntriesCheckpoint, boolean trace) {
    TwoTable(Atable, Btable, Ctable, CTtable, TwoTableIterator.DOT_TYPE.ROW_COLF_COLQ_MATCH,
        multOp, plusOp, rowFilter, colFilterAT, colFilterB,
        true, true, numEntriesCheckpoint, trace);
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
   * @param colFilterAT          Column qualifier subset of AT. Null means run on all columns.
   * @param colFilterB           Column qualifier subset of B. Null means run on all columns.
   * @param numEntriesCheckpoint # of entries before we emit a checkpoint entry from the scan. -1 means no monitoring.
   * @param trace                Enable server-side performance tracing.
   */
  public void TableMult(String ATtable, String Btable, String Ctable, String CTtable,
                        Class<? extends IMultiplyOp> multOp, IteratorSetting plusOp,
                        Collection<Range> rowFilter,
                        String colFilterAT, String colFilterB,
                        int numEntriesCheckpoint, boolean trace) {
    TwoTable(ATtable, Btable, Ctable, CTtable, TwoTableIterator.DOT_TYPE.ROW_CARTESIAN,
            multOp, plusOp, rowFilter, colFilterAT, colFilterB,
        false, false, numEntriesCheckpoint, trace);
  }

  public void TwoTable(String ATtable, String Btable, String Ctable, String CTtable, TwoTableIterator.DOT_TYPE dot,
                        Class<? extends IMultiplyOp> multOp, IteratorSetting plusOp,
                        Collection<Range> rowFilter,
                        String colFilterAT, String colFilterB,
                        boolean emitNoMatchA, boolean emitNoMatchB,
                        int numEntriesCheckpoint, boolean trace) {
    if (ATtable == null || ATtable.isEmpty())
      throw new IllegalArgumentException("Please specify table AT. Given: " + ATtable);
    if (Btable == null || Btable.isEmpty())
      throw new IllegalArgumentException("Please specify table B. Given: " + Btable);
    // Prevent possibility for infinite loop:
    if (ATtable.equals(Ctable))
      throw new UnsupportedOperationException("Could lead to unpredictable results: ATtable=Ctable=" + ATtable);
    if (Btable.equals(Ctable))
      throw new UnsupportedOperationException("Could lead to unpredictable results: Btable=Ctable=" + Btable);
    if (ATtable.equals(CTtable))
      throw new UnsupportedOperationException("Could lead to unpredictable results: ATtable=CTtable=" + ATtable);
    if (Btable.equals(CTtable))
      throw new UnsupportedOperationException("Could lead to unpredictable results: Btable=CTtable=" + Btable);

    if (Ctable != null && Ctable.isEmpty())
      Ctable = null;
    if (CTtable != null && CTtable.isEmpty())
      CTtable = null;
    if (Ctable == null && CTtable == null)
      log.warn("Streaming back result of multiplication to client does not guarantee correctness." +
          "In particular, if Accumulo destroys, re-inits and re-seeks an iterator stack, the stack may not recover.");

    if (multOp == null)
      throw new IllegalArgumentException("multOp is required but given null");
    if (rowFilter != null && rowFilter.isEmpty())
      rowFilter = null;

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

    opt.put("AT.emitNoMatch", Boolean.toString(emitNoMatchA));
    opt.put("B.emitNoMatch", Boolean.toString(emitNoMatchB));

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

    if (rowFilter != null) {
      if (Ctable != null || CTtable != null) {
        opt.put("C.rowRanges", GraphuloUtil.rangesToD4MString(rowFilter)); // translate row filter to D4M notation
        bs.setRanges(Collections.singleton(new Range()));
      } else
        bs.setRanges(rowFilter);
    } else
      bs.setRanges(Collections.singleton(new Range()));

    // TODO P2: Assign priority and name dynamically, checking for conflicts.
    IteratorSetting itset = new IteratorSetting(5, TableMultIterator.class, opt);
    bs.addScanIterator(itset);

    GraphuloUtil.applyGeneralColumnFilter(colFilterB, bs, 4);

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
          int numEntries = RemoteWriteIterator.decodeValue(entry.getValue(), null);
          System.out.println(entry.getKey() + " -> " + numEntries + "entries processed");
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
   * @param Atable      Name of Accumulo table.
   * @param v0          Starting nodes, like "a,f,b,c,". Null or empty string "" means start from all nodes.
   *                    v0 may be a range of nodes like "c,:,e,g,k,:,".
   * @param k           Number of steps
   * @param Rtable      Name of table to store result. Null means don't store the result.
   * @param RTtable     Name of table to store transpose of result. Null means don't store the transpose.
   * @param ADegtable   Name of table holding out-degrees for A. Leave null to filter on the fly with
   *                    the {@link SmallLargeRowFilter}, or do no filtering if minDegree=0 and maxDegree=Integer.MAX_VALUE.
   * @param degColumn   Name of column for out-degrees in ADegtable like "deg". Null means the empty column "".
   *                    If degInColQ==true, this is the prefix before the numeric portion of the column like "deg|", and null means no prefix. Unused if ADegtable is null.
   * @param degInColQ   True means degree is in the Column Qualifier. False means degree is in the Value.
   *                    Unused if ADegtable is null.
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

    if (degColumn == null)
      degColumn = "";
    Text degColumnText = new Text(degColumn);

    if (plusOp != null && plusOp.getPriority() >= 20)
      log.warn("Sum iterator setting is >=20. Are you sure you want the priority after the default Versioning iterator priority? " + plusOp);
    if (v0 == null || v0.isEmpty())
      v0 = ":\t";
    Collection<Text> vktexts = new HashSet<>(); //v0 == null ? null : GraphuloUtil.d4mRowToTexts(v0);
    char sep = v0.charAt(v0.length() - 1);

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
        log.error("error trying to create R table transpose " + RTtable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }

    Map<String, String> opt = new HashMap<>();
//    opt.put("trace", String.valueOf(trace)); // logs timing on server
    opt.put("reducer", GatherColQReducer.class.getName());
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
      bs = connector.createBatchScanner(Atable, Authorizations.EMPTY, 50); // TODO P2: set number of batch scan threads
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
    bs.setRanges(Collections.singleton(new Range()));

    try {
      long degTime = 0, scanTime = 0;
      for (int thisk = 1; thisk <= k; thisk++) {
        if (trace)
          if (thisk == 1)
            System.out.println("First step: v0 is " + v0);
          else
            System.out.println("k=" + thisk + " before filter" +
                (vktexts.size() > 5 ? " #=" + String.valueOf(vktexts.size()) : ": " + vktexts.toString()));

        bs.clearScanIterators();

        if (needDegreeFiltering && ADegtable != null) { // use degree table
          long t1 = System.currentTimeMillis(), dur;
          vktexts = thisk == 1
              ? filterTextsDegreeTable(ADegtable, degColumnText, degInColQ, minDegree, maxDegree, vktexts, GraphuloUtil.d4mRowToRanges(v0))
              : filterTextsDegreeTable(ADegtable, degColumnText, degInColQ, minDegree, maxDegree, vktexts);
          dur = System.currentTimeMillis() - t1;
          degTime += dur;
          if (trace) {
            System.out.println("Degree Lookup Time: " + dur + " ms");
            System.out.println("k=" + thisk + " after  filter" +
                (vktexts.size() > 5 ? " #=" + String.valueOf(vktexts.size()) : ": " + vktexts.toString()));
          }

          if (vktexts.isEmpty())
            break;
          opt.put("rowRanges", GraphuloUtil.textsToD4mString(vktexts, sep));

        } else {  // no degree table or no filtering
          if (thisk == 1)
            opt.put("rowRanges", v0);
          else
            opt.put("rowRanges", GraphuloUtil.textsToD4mString(vktexts, sep));
          if (needDegreeFiltering) // filtering but no degree table
            bs.addScanIterator(itsetDegreeFilter);
        }

        IteratorSetting itset = new IteratorSetting(4, RemoteWriteIterator.class, opt);
        bs.addScanIterator(itset);

        GatherColQReducer reducer = new GatherColQReducer();
        reducer.init(Collections.<String, String>emptyMap(), null);
        long t2 = System.currentTimeMillis();
        for (Map.Entry<Key, Value> entry : bs) {
//        System.out.println("A Entry: "+entry.getKey() + " -> " + entry.getValue());
          RemoteWriteIterator.decodeValue(entry.getValue(), reducer);
        }
        long dur = System.currentTimeMillis() - t2;
        scanTime += dur;

        vktexts.clear();
//      vktexts.addAll(reducer.get());
        for (String uk : reducer.get()) {
          vktexts.add(new Text(uk));
        }
        if (trace)
          System.out.println("BatchScan/Iterator Time: " + dur + " ms");
        if (vktexts.isEmpty())
          break;
      }

      if (trace) {
        System.out.println("Total Degree Lookup Time: " + degTime + " ms");
        System.out.println("Total BatchScan/Iterator Time: " + scanTime + " ms");
      }
    } finally {
      bs.close();
    }

    return GraphuloUtil.textsToD4mString(vktexts, sep);
  }

  /**
   * Modifies texts in place, removing the entries that are out of range.
   * Decodes degrees using {@link LongCombiner#STRING_ENCODER}.
   * We disqualify a node if we cannot parse its degree.
   * Does nothing if texts is null or the empty collection.
   * Todo: Add a local cache parameter for known good nodes and known bad nodes, so that we don't have to look them up.
   *
   * @param degColumnText   Name of the degree column qualifier. Blank/null means fetch the empty ("") column.
   * @param degInColQ False means degree in value. True means degree in column qualifier and that degColumnText is a prefix before the numeric portion of the column qualifier degree.
   * @return The same texts object, with nodes that fail the degree filter removed.
   */
  private Collection<Text> filterTextsDegreeTable(String ADegtable, Text degColumnText, boolean degInColQ,
                                                  int minDegree, int maxDegree,
                                                  Collection<Text> texts) {
    if (texts == null || texts.isEmpty())
      return texts;
    return filterTextsDegreeTable(ADegtable, degColumnText, degInColQ,
        minDegree, maxDegree, texts, GraphuloUtil.textsToRanges(texts));
  }

  /** Used when thisk==1, on first call to BFS. */
  private Collection<Text> filterTextsDegreeTable(String ADegtable, Text degColumnText, boolean degInColQ,
                                                  int minDegree, int maxDegree,
                                                  Collection<Text> texts, Collection<Range> ranges) {
    if (ranges == null || ranges.isEmpty())
      return texts;
    boolean addToTexts = false;
    if (texts == null)
      throw new IllegalArgumentException("texts is null");
    if (texts.isEmpty())
      addToTexts = true;
    if (degColumnText.getLength() == 0)
      degColumnText = null;
    TableOperations tops = connector.tableOperations();
    assert ADegtable != null && !ADegtable.isEmpty() && minDegree > 0 && maxDegree >= minDegree
        && tops.exists(ADegtable);
    BatchScanner bs;
    try {
      bs = connector.createBatchScanner(ADegtable, Authorizations.EMPTY, 4); // TODO P2: set number of batch scan threads
    } catch (TableNotFoundException e) {
      log.error("crazy", e);
      throw new RuntimeException(e);
    }
    bs.setRanges(ranges);
    if (!degInColQ)
      bs.fetchColumn(EMPTY_TEXT, degColumnText == null ? EMPTY_TEXT : degColumnText);
    else if (degColumnText != null) {
      // single range: deg|,:,deg},
      IteratorSetting itset = new IteratorSetting(1, ColumnSliceFilter.class);
      ColumnSliceFilter.setSlice(itset, degColumnText.toString(),
          true, Range.followingPrefix(degColumnText).toString(), false);
      bs.addScanIterator(itset);
    }

    try {
      Text badRow = new Text();
      for (Map.Entry<Key, Value> entry : bs) {
        boolean bad = false;
//      log.debug("Deg Entry: " + entry.getKey() + " -> " + entry.getValue());
        try {
          long deg;
          if (degInColQ) {
            if (degColumnText != null) {
              String degString = GraphuloUtil.stringAfter(degColumnText.getBytes(), entry.getKey().getColumnQualifierData().getBackingArray());
              if (degString == null) // should never occur since we use a column filter
                continue;
              else
                deg = Long.parseLong(degString);
            } else
              deg = LongCombiner.STRING_ENCODER.decode(entry.getKey().getColumnQualifierData().getBackingArray());
          } else
            deg = LongCombiner.STRING_ENCODER.decode(entry.getValue().get());
          if (deg < minDegree || deg > maxDegree)
            bad = true;
        } catch (NumberFormatException | ValueFormatException e) {
          log.warn("Trouble parsing degree entry as long; assuming bad degree: " + entry.getKey() + " -> " + entry.getValue(), e);
          bad = true;
        }

        if (!addToTexts && bad) {
          boolean remove = texts.remove(entry.getKey().getRow(badRow));
          if (!remove)
            log.warn("Unrecognized entry with bad degree; cannot remove: " + entry.getKey() + " -> " + entry.getValue());
        } else if (addToTexts) {
          texts.add(entry.getKey().getRow()); // need new Text object
        }
      }
    } finally {
      bs.close();
    }
    return texts;
  }

  /**
   * Out-degree-filtered Breadth First Search on Incidence table.
   * Conceptually k iterations of: v0 ==startPrefix==> edge ==endPrefix==> v1.
   *
   * @param Etable        Incidence table; rows are edges, column qualifiers are nodes.
   * @param v0            Starting vertices, like "v0,v5,v6,".
   *                      v0 may be a range of nodes like "c,:,e,g,k,:,".
   * @param k             Number of steps.
   * @param Rtable        Name of table to store result. Null means don't store the result.
   * @param RTtable       Name of table to store transpose of result. Null means don't store the transpose.
   * @param startPrefix   Prefix of 'start' of an edge including separator, e.g. 'out|'
   * @param endPrefix     Prefix of 'end' of an edge including separator, e.g. 'in|'
   * @param ETDegtable    Name of table holding out-degrees for ET. Must be provided if degree filtering is used.
   * @param degColumn   Name of column for out-degrees in ETDegtable like "deg". Null means the empty column "".
   *                    If degInColQ==true, this is the prefix before the numeric portion of the column like "deg|", and null means no prefix.
   *                    Unused if ETDegtable is null.
   * @param degInColQ   True means degree is in the Column Qualifier. False means degree is in the Value. Unused if ETDegtable is null.
   * @param minDegree     Minimum out-degree. Checked before doing any searching, at every step, from ADegtable. Pass 0 for no filtering.
   * @param maxDegree     Maximum out-degree. Checked before doing any searching, at every step, from ADegtable. Pass Integer.MAX_VALUE for no filtering.
   * @param plusOp      An SKVI to apply to the result table that "sums" values. Not applied if null.
   * @param trace       Enable server-side performance tracing.
   * @return              The nodes reachable in exactly k steps from v0.
   */
  public String  EdgeBFS(String Etable, String v0, int k, String Rtable, String RTtable,
               String startPrefix, String endPrefix,
               String ETDegtable, String degColumn, boolean degInColQ, int minDegree, int maxDegree,
               IteratorSetting plusOp, boolean trace) {
    boolean needDegreeFiltering = minDegree > 0 || maxDegree < Integer.MAX_VALUE;
    if (Etable == null || Etable.isEmpty())
      throw new IllegalArgumentException("Please specify Incidence table. Given: " + Etable);
    if (needDegreeFiltering && (ETDegtable == null || ETDegtable.isEmpty()))
      throw new IllegalArgumentException("Need degree table for EdgeBFS. Given: "+ETDegtable);
    if (Rtable != null && Rtable.isEmpty())
      Rtable = null;
    if (RTtable != null && RTtable.isEmpty())
      RTtable = null;
    if (minDegree < 1)
      minDegree = 1;
    if (maxDegree < minDegree)
      throw new IllegalArgumentException("maxDegree=" + maxDegree + " should be >= minDegree=" + minDegree);

    if (degColumn == null)
      degColumn = "";
    Text degColumnText = new Text(degColumn);
    if (plusOp != null && plusOp.getPriority() >= 20)
      log.warn("Sum iterator setting is >=20. Are you sure you want the priority after the default Versioning iterator priority? " + plusOp);
    if (v0 == null || v0.isEmpty())
      v0 = ":\t";
    Collection<Text> vktexts = new HashSet<>();
    char sep = v0.charAt(v0.length() - 1);

    TableOperations tops = connector.tableOperations();
    if (!tops.exists(Etable))
      throw new IllegalArgumentException("Table E does not exist. Given: " + Etable);
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
        log.error("error trying to create R table transpose " + RTtable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }

    Map<String, String> opt = new HashMap<>();
//    opt.put("trace", String.valueOf(trace)); // logs timing on server
//    opt.put("gatherColQs", "true");  No gathering right now.  Need to implement more general gathering function on RemoteWriteIterator.
    opt.put("dot", TwoTableIterator.DOT_TYPE.ROW_CARTESIAN.name());
    opt.put("multiplyOp", EdgeBFSMultiply.class.getName());
    String instance = connector.getInstance().getInstanceName();
    String zookeepers = connector.getInstance().getZooKeepers();
    String user = connector.whoami();
//    opt.put("AT.zookeeperHost", zookeepers);
//    opt.put("AT.instanceName", instance);
    opt.put("AT.tableName", TwoTableIterator.CLONESOURCE_TABLENAME);
//    opt.put("AT.username", user);
//    opt.put("AT.password", new String(password.getPassword()));


    if (Rtable != null || RTtable != null) {
      opt.put("C.zookeeperHost", zookeepers);
      opt.put("C.instanceName", instance);
      if (Rtable != null)
        opt.put("C.tableName", Rtable);
      if (RTtable != null)
        opt.put("C.tableNameTranspose", RTtable);
      opt.put("C.username", user);
      opt.put("C.password", new String(password.getPassword()));
      opt.put("C.reducer", EdgeBFSReducer.class.getName());
      opt.put("C.reducer.opt.inColumnPrefix", endPrefix);
//      opt.put("C.numEntriesCheckpoint", String.valueOf(numEntriesCheckpoint));

      if (Rtable != null && plusOp != null)
        GraphuloUtil.applyIteratorSoft(plusOp, tops, Rtable);
      if (RTtable != null && plusOp != null)
        GraphuloUtil.applyIteratorSoft(plusOp, tops, RTtable);
    }

    BatchScanner bs;
    try {
      bs = connector.createBatchScanner(Etable, Authorizations.EMPTY, 2); // TODO P2: set number of batch scan threads
    } catch (TableNotFoundException e) {
      log.error("crazy", e);
      throw new RuntimeException(e);
    }

    String colFilterB = prependStartPrefix(endPrefix, sep, null);
    log.debug("fetchColumn "+colFilterB);
//    bs.fetchColumn(EMPTY_TEXT, new Text(GraphuloUtil.prependStartPrefix(endPrefix, v0, null)));

    try {
      long degTime = 0, scanTime = 0;
      for (int thisk = 1; thisk <= k; thisk++) {
        if (trace)
          if (thisk == 1)
            System.out.println("First step: v0 is " + v0);
          else
            System.out.println("k=" + thisk + " before filter" +
                (vktexts.size() > 5 ? " #=" + String.valueOf(vktexts.size()) : ": " + vktexts.toString()));

        if (needDegreeFiltering) { // use degree table
          long t1 = System.currentTimeMillis(), dur;
          vktexts = thisk == 1
              ? filterTextsDegreeTable(ETDegtable, degColumnText, degInColQ, minDegree, maxDegree, vktexts, GraphuloUtil.d4mRowToRanges(v0))
              : filterTextsDegreeTable(ETDegtable, degColumnText, degInColQ, minDegree, maxDegree, vktexts);
          dur = System.currentTimeMillis() - t1;
          degTime += dur;
          if (trace) {
            System.out.println("Degree Lookup Time: " + dur + " ms");
            System.out.println("k=" + thisk + " after  filter" +
                (vktexts.size() > 5 ? " #=" + String.valueOf(vktexts.size()) : ": " + vktexts.toString()));
          }

          if (vktexts.isEmpty())
            break;
          opt.put("AT.colFilter", prependStartPrefix(startPrefix, sep, vktexts));

        } else {  // no filtering
          if (thisk == 1)
            opt.put("AT.colFilter", GraphuloUtil.padD4mString(startPrefix, "", v0));
          else
            opt.put("AT.colFilter", prependStartPrefix(startPrefix, sep, vktexts));
        }
        log.debug("AT.colFilter: " + opt.get("AT.colFilter"));

        bs.setRanges(Collections.singleton(new Range()));
        bs.clearScanIterators();
        bs.clearColumns();
//        GraphuloUtil.applyGeneralColumnFilter(colFilterB, bs, 4, false);
        opt.put("B.colFilter", colFilterB);
        IteratorSetting itset = new IteratorSetting(5, TableMultIterator.class, opt);
        bs.addScanIterator(itset);

        EdgeBFSReducer reducer = new EdgeBFSReducer();
        reducer.init(Collections.singletonMap("inColumnPrefix", endPrefix), null);
        long t2 = System.currentTimeMillis();
        for (Map.Entry<Key, Value> entry : bs) {
//        System.out.println("A Entry: "+entry.getKey() + " -> " + entry.getValue());
          RemoteWriteIterator.decodeValue(entry.getValue(), reducer);
        }
        long dur = System.currentTimeMillis() - t2;
        scanTime += dur;

        vktexts.clear();
        for (String uk : reducer.get()) {
          vktexts.add(new Text(uk));
        }
        if (trace)
          System.out.println("BatchScan/Iterator Time: " + dur + " ms");
        if (vktexts.isEmpty())
          break;
      }

      if (trace) {
        System.out.println("Total Degree Lookup Time: " + degTime + " ms");
        System.out.println("Total BatchScan/Iterator Time: " + scanTime + " ms");
      }
    } finally {
      bs.close();
    }
    return GraphuloUtil.textsToD4mString(vktexts, sep);

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


  /**
   * Todo? Replace with {@link Range#prefix} or {@link Range#followingPrefix(Text)}.
   * May break with unicode.
   * @param prefix e.g. "out|"
   * @param vktexts Set of nodes like "v1,v3,v0,"
   * @return "out|v1,out|v3,out|v0," or "out|,:,out}," if vktexts is null or empty
   */
  static String prependStartPrefix(String prefix, char sep, Collection<Text> vktexts) {
    if (vktexts == null || vktexts.isEmpty()) {
//      byte[] orig = prefix.getBytes();
//      byte[] newb = new byte[orig.length*2+4];
//      System.arraycopy(orig,0,newb,0,orig.length);
//      newb[orig.length] = (byte)sep;
//      newb[orig.length+1] = ':';
//      newb[orig.length+2] = (byte)sep;
//      System.arraycopy(orig,0,newb,orig.length+3,orig.length-1);
//      newb[orig.length*2+2] = (byte) (orig[orig.length-1]+1);
//      newb[orig.length*2+3] = (byte)sep;
//      return new String(newb);
      Text pt = new Text(prefix);
      Text after = Range.followingPrefix(pt);
      return prefix + sep + ':' + sep + after.toString() + sep;
    } else {
      StringBuilder ret = new StringBuilder();
      for (Text vktext : vktexts) {
        ret.append(prefix).append(vktext.toString()).append(sep);
      }
      return ret.toString();
    }
  }


  /**
   * Single-table Breadth First Search. Sums entries into Rtable from each step of the BFS.
   * Note that this version does not copy the in-degrees into the result table.
   *
   * @param Stable         Name of Accumulo table.
   * @param edgeColumn     Column that edges are stored in.
   * @param edgeSep        Character used to separate edges in the row key, e.g. '|'.
   * @param v0             Starting nodes, like "a,f,b,c,". Null or empty string "" means start from all nodes.
   *                       v0 may be a range of nodes like "c,:,e,g,k,:,".
   * @param k              Number of steps
   * @param Rtable         Name of table to store result. Null means don't store the result.
   * @param SDegtable      Name of table holding out-degrees for S. This should almost always be the same as Stable.
   *                       Can set to null only if no filtering is necessary, e.g.,
   *                       if minDegree=0 and maxDegree=Integer.MAX_VALUE.
   * @param degColumn      Name of column for out-degrees in SDegtable like "out". Null means the empty column "".
   *                       If degInColQ==true, this is the prefix before the numeric portion of the column
   *                       like "out|", and null means no prefix. Unused if ADegtable is null.
   * @param degInColQ      True means degree is in the Column Qualifier. False means degree is in the Value.
   *                       Unused if SDegtable is null.
   * @param copyOutDegrees True means copy out-degrees from Stable to Rtable. This must be false if SDegtable is different from Stable. (could remove restriction in future)
   * @param minDegree      Minimum out-degree. Checked before doing any searching, at every step, from SDegtable. Pass 0 for no filtering.
   * @param maxDegree      Maximum out-degree. Checked before doing any searching, at every step, from SDegtable. Pass Integer.MAX_VALUE for no filtering.
   * @param plusOp         An SKVI to apply to the result table that "sums" values. Not applied if null.
   *                       Be careful: this affects degrees in the result table as well as normal entries.
   * @param trace          Enable server-side performance tracing.
   * @return The nodes reachable in exactly k steps from v0.
   */
  @SuppressWarnings("unchecked")
  public String SingleBFS(String Stable, String edgeColumn, char edgeSep,
                          String v0, int k, String Rtable,
                          String SDegtable, String degColumn, boolean degInColQ,
                          boolean copyOutDegrees, int minDegree, int maxDegree,
                          IteratorSetting plusOp, boolean trace) {
    boolean needDegreeFiltering = minDegree > 0 || maxDegree < Integer.MAX_VALUE;
    if (Stable == null || Stable.isEmpty())
      throw new IllegalArgumentException("Please specify Single-table. Given: " + Stable);
    if (needDegreeFiltering && (SDegtable == null || SDegtable.isEmpty()))
      throw new IllegalArgumentException("Please specify SDegtable since filtering is required. Given: " + Stable);
    if (Rtable != null && Rtable.isEmpty())
      Rtable = null;
    if (edgeColumn == null)
      edgeColumn = "";
    Text edgeColumnText = new Text(edgeColumn);
    if (copyOutDegrees && !SDegtable.equals(Stable))
      throw new IllegalArgumentException("Stable and SDegtable must be the same when copying out-degrees. Stable: " + Stable + " SDegtable: " + SDegtable);
    if (minDegree < 1)
      minDegree = 1;
    if (maxDegree < minDegree)
      throw new IllegalArgumentException("maxDegree=" + maxDegree + " should be >= minDegree=" + minDegree);
    String edgeSepStr = String.valueOf(edgeSep);

    if (degColumn == null)
      degColumn = "";
    Text degColumnText = new Text(degColumn);

    if (plusOp != null && plusOp.getPriority() >= 20)
      log.warn("Sum iterator setting is >=20. Are you sure you want the priority after the default Versioning iterator priority? " + plusOp);
    if (v0 == null || v0.isEmpty())
      v0 = ":\t";
    Collection<Text> vktexts = new HashSet<>(); //v0 == null ? null : GraphuloUtil.d4mRowToTexts(v0);
    char sep = v0.charAt(v0.length() - 1);

    TableOperations tops = connector.tableOperations();
    if (!tops.exists(Stable))
      throw new IllegalArgumentException("Table A does not exist. Given: " + Stable);
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

    Map<String, String> opt = new HashMap<>();
//    opt.put("trace", String.valueOf(trace)); // logs timing on server
    opt.put("reducer", SingleBFSReducer.class.getName());
    opt.put("reducer.opt." + SingleBFSReducer.EDGE_SEP, edgeSepStr);
    if (Rtable != null) {
      String instance = connector.getInstance().getInstanceName();
      String zookeepers = connector.getInstance().getZooKeepers();
      String user = connector.whoami();
      opt.put("zookeeperHost", zookeepers);
      opt.put("instanceName", instance);
//        if (Rtable != null)
      opt.put("tableName", Rtable);
      opt.put("username", user);
      opt.put("password", new String(password.getPassword()));

      if (/*Rtable != null &&*/ plusOp != null)
        GraphuloUtil.applyIteratorSoft(plusOp, tops, Rtable);
    }
    BatchScanner bs;
    try {
      bs = connector.createBatchScanner(Stable, Authorizations.EMPTY, 50); // TODO P2: set number of batch scan threads
    } catch (TableNotFoundException e) {
      log.error("crazy", e);
      throw new RuntimeException(e);
    }
    bs.setRanges(Collections.singleton(new Range()));

    bs.fetchColumn(EMPTY_TEXT, edgeColumnText);
    if (copyOutDegrees)
      bs.fetchColumn(EMPTY_TEXT, degColumnText);

    try {
      long degTime = 0, scanTime = 0;
      for (int thisk = 1; thisk <= k; thisk++) {
        if (trace)
          if (thisk == 1)
            System.out.println("First step: v0 is " + v0);
          else
            System.out.println("k=" + thisk + " before filter" +
                (vktexts.size() > 5 ? " #=" + String.valueOf(vktexts.size()) : ": " + vktexts.toString()));

        bs.clearScanIterators();

        if (needDegreeFiltering /*&& SDegtable != null*/) { // use degree table
          long t1 = System.currentTimeMillis(), dur;
          vktexts = thisk == 1
              ? filterTextsDegreeTable(SDegtable, degColumnText, degInColQ, minDegree, maxDegree, vktexts, GraphuloUtil.d4mRowToRanges(v0))
              : filterTextsDegreeTable(SDegtable, degColumnText, degInColQ, minDegree, maxDegree, vktexts);
          dur = System.currentTimeMillis() - t1;
          degTime += dur;
          if (trace) {
            System.out.println("Degree Lookup Time: " + dur + " ms");
            System.out.println("k=" + thisk + " after  filter" +
                (vktexts.size() > 5 ? " #=" + String.valueOf(vktexts.size()) : ": " + vktexts.toString()));
          }

          if (vktexts.isEmpty())
            break;
          String s = GraphuloUtil.singletonsAsPrefix(vktexts, sep);
          opt.put("rowRanges", GraphuloUtil.singletonsAsPrefix(vktexts, sep));

        } else {  // no filtering
          if (thisk == 1)
            opt.put("rowRanges",
                GraphuloUtil.singletonsAsPrefix(v0));
          else
            opt.put("rowRanges", GraphuloUtil.singletonsAsPrefix(vktexts, sep));
        }

        IteratorSetting itset = new IteratorSetting(4, RemoteWriteIterator.class, opt);
        bs.addScanIterator(itset);

        SingleBFSReducer reducer = new SingleBFSReducer();
        reducer.init(Collections.singletonMap(SingleBFSReducer.EDGE_SEP, edgeSepStr), null);
        long t2 = System.currentTimeMillis();
        for (Map.Entry<Key, Value> entry : bs) {
//        System.out.println("A Entry: "+entry.getKey() + " -> " + entry.getValue());
          RemoteWriteIterator.decodeValue(entry.getValue(), reducer);
        }
        long dur = System.currentTimeMillis() - t2;
        scanTime += dur;

        vktexts.clear();
//      vktexts.addAll(reducer.get());
        for (String uk : reducer.get()) {
          vktexts.add(new Text(uk));
        }
        if (trace)
          System.out.println("BatchScan/Iterator Time: " + dur + " ms");
        if (vktexts.isEmpty())
          break;
      }

      if (trace) {
        System.out.println("Total Degree Lookup Time: " + degTime + " ms");
        System.out.println("Total BatchScan/Iterator Time: " + scanTime + " ms");
      }
    } finally {
      bs.close();
    }

    return GraphuloUtil.textsToD4mString(vktexts, sep);
  }

}
