package edu.mit.ll.graphulo;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.apply.JaccardDegreeApply;
import edu.mit.ll.graphulo.apply.KeyRetainOnlyApply;
import edu.mit.ll.graphulo.apply.RandomTopicApply;
import edu.mit.ll.graphulo.ewise.EWiseOp;
import edu.mit.ll.graphulo.reducer.EdgeBFSReducer;
import edu.mit.ll.graphulo.reducer.GatherColQReducer;
import edu.mit.ll.graphulo.reducer.Reducer;
import edu.mit.ll.graphulo.reducer.SingleBFSReducer;
import edu.mit.ll.graphulo.rowmult.CartesianRowMultiply;
import edu.mit.ll.graphulo.rowmult.EdgeBFSMultiply;
import edu.mit.ll.graphulo.rowmult.LineRowMultiply;
import edu.mit.ll.graphulo.rowmult.MultiplyOp;
import edu.mit.ll.graphulo.rowmult.SelectorRowMultiply;
import edu.mit.ll.graphulo.simplemult.ConstantTwoScalar;
import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.simplemult.MathTwoScalar.ScalarOp;
import edu.mit.ll.graphulo.simplemult.MathTwoScalar.ScalarType;
import edu.mit.ll.graphulo.skvi.CountAllIterator;
import edu.mit.ll.graphulo.skvi.InverseMatrixIterator;
import edu.mit.ll.graphulo.skvi.MinMaxValueFilter;
import edu.mit.ll.graphulo.skvi.RemoteWriteIterator;
import edu.mit.ll.graphulo.skvi.SeekFilterIterator;
import edu.mit.ll.graphulo.skvi.SingleTransposeIterator;
import edu.mit.ll.graphulo.skvi.SmallLargeRowFilter;
import edu.mit.ll.graphulo.skvi.TableMultIterator;
import edu.mit.ll.graphulo.skvi.TriangularFilter;
import edu.mit.ll.graphulo.skvi.TwoTableIterator;
import edu.mit.ll.graphulo.util.DebugUtil;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.MemMatrixUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.accumulo.core.iterators.user.ColumnSliceFilter;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealMatrixChangingVisitor;
import org.apache.hadoop.io.Text;
import org.apache.htrace.Sampler;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Holds a {@link org.apache.accumulo.core.client.Connector} to an Accumulo instance for calling core client Graphulo operations.
 */
public class Graphulo {
  private static final Logger log = LogManager.getLogger(Graphulo.class);

  public static final IteratorSetting PLUS_ITERATOR_BIGDECIMAL =
            MathTwoScalar.combinerSetting(6, null, ScalarOp.PLUS, ScalarType.BIGDECIMAL);
  public static final IteratorSetting PLUS_ITERATOR_LONG =
            MathTwoScalar.combinerSetting(6, null, ScalarOp.PLUS, ScalarType.LONG);

  protected Connector connector;
  protected PasswordToken password;

  public Graphulo(Connector connector, PasswordToken password) {
    this.connector = connector;
    this.password = password;
    checkCredentials();
    checkGraphuloInstalled();
  }

  /**
   * Check password works for this user.
   */
  private void checkCredentials() {
    try {
      if (!connector.securityOperations().authenticateUser(connector.whoami(), password))
        throw new IllegalArgumentException("instance " + connector.getInstance().getInstanceName() + ": bad username " + connector.whoami() + " with password " + new String(password.getPassword()));
    } catch (AccumuloException e) {
      throw new IllegalArgumentException("instance " + connector.getInstance().getInstanceName() + ": error with username " + connector.whoami() + " with password " + new String(password.getPassword()), e);
    } catch (AccumuloSecurityException e) {
      throw new IllegalArgumentException("instance " + connector.getInstance().getInstanceName() + ": error with username " + connector.whoami() + " with password " + new String(password.getPassword()), e);
    }
  }

  /** Check the Graphulo classes are installed on the Accumulo server. */
  private void checkGraphuloInstalled() {
//    connector.tableOperations().testClassLoad()
    // Works on a table-level basis...
  }

  private static final Text EMPTY_TEXT = new Text();


  public long TableMult(String ATtable, String Btable, String Ctable, String CTtable,
                        Class<? extends MultiplyOp> multOp, IteratorSetting plusOp) {
    return TableMult(ATtable, Btable, Ctable, CTtable, -1, multOp, null, plusOp, null, null, null,
        false, false, -1, false);
  }

  public long TableMult(String ATtable, String Btable, String Ctable, String CTtable,
                        Class<? extends MultiplyOp> multOp, IteratorSetting plusOp,
                        Collection<Range> rowFilter,
                        String colFilterAT, String colFilterB) {
    return TableMult(ATtable, Btable, Ctable, CTtable, -1, multOp, null, plusOp, rowFilter, colFilterAT, colFilterB,
        false, false, -1, false);
  }

  public long SpEWiseX(String Atable, String Btable, String Ctable, String CTtable,
                       int BScanIteratorPriority,
                       Class<? extends EWiseOp> multOp, Map<String, String> multOpOptions,
                       IteratorSetting plusOp,
                       Collection<Range> rowFilter,
                       String colFilterAT, String colFilterB,
                       int numEntriesCheckpoint, boolean trace) {
    return TwoTableEWISE(Atable, Btable, Ctable, CTtable, BScanIteratorPriority,
        multOp, multOpOptions, plusOp, rowFilter, colFilterAT, colFilterB,
        false, false, Collections.<IteratorSetting>emptyList(),
        Collections.<IteratorSetting>emptyList(), Collections.<IteratorSetting>emptyList(),
        null, null,
        numEntriesCheckpoint, trace);
  }

  public long SpEWiseX(String Atable, String Btable, String Ctable, String CTtable,
                       int BScanIteratorPriority,
                       Class<? extends EWiseOp> multOp, Map<String, String> multOpOptions,
                       IteratorSetting plusOp,
                       Collection<Range> rowFilter,
                       String colFilterAT, String colFilterB,
                       List<IteratorSetting> iteratorsBeforeA, List<IteratorSetting> iteratorsBeforeB,
                       List<IteratorSetting> iteratorsAfterTwoTable,
                       Reducer reducer, Map<String,String> reducerOpts,
                       int numEntriesCheckpoint, boolean trace) {
    return TwoTableEWISE(Atable, Btable, Ctable, CTtable, BScanIteratorPriority,
        multOp, multOpOptions, plusOp, rowFilter, colFilterAT, colFilterB,
        false, false, iteratorsBeforeA,
        iteratorsBeforeB, iteratorsAfterTwoTable,
        reducer, reducerOpts,
        numEntriesCheckpoint, trace);
  }

  public long SpEWiseSum(String Atable, String Btable, String Ctable, String CTtable,
                         int BScanIteratorPriority,
                         Class<? extends EWiseOp> multOp, Map<String, String> multOpOptions,
                         IteratorSetting plusOp,
                         Collection<Range> rowFilter,
                         String colFilterAT, String colFilterB,
                         int numEntriesCheckpoint, boolean trace) {
    if (multOp.equals(MathTwoScalar.class) && multOpOptions == null)
      multOpOptions = MathTwoScalar.optionMap(ScalarOp.PLUS, ScalarType.BIGDECIMAL); // + by default for SpEWiseSum
    return TwoTableEWISE(Atable, Btable, Ctable, CTtable, BScanIteratorPriority,
        multOp, multOpOptions, plusOp, rowFilter, colFilterAT, colFilterB,
        true, true, Collections.<IteratorSetting>emptyList(),
        Collections.<IteratorSetting>emptyList(), Collections.<IteratorSetting>emptyList(),
        null, null,
        numEntriesCheckpoint, trace);
  }

  public long SpEWiseSum(String Atable, String Btable, String Ctable, String CTtable,
                         int BScanIteratorPriority,
                         Class<? extends EWiseOp> multOp, Map<String, String> multOpOptions,
                         IteratorSetting plusOp,
                         Collection<Range> rowFilter,
                         String colFilterAT, String colFilterB,
                         List<IteratorSetting> iteratorsBeforeA, List<IteratorSetting> iteratorsBeforeB,
                         List<IteratorSetting> iteratorsAfterTwoTable,
                         Reducer reducer, Map<String,String> reducerOpts,
                         int numEntriesCheckpoint, boolean trace) {
    if (multOp.equals(MathTwoScalar.class) && multOpOptions == null)
      multOpOptions = MathTwoScalar.optionMap(ScalarOp.PLUS, ScalarType.BIGDECIMAL); // + by default for SpEWiseSum
    return TwoTableEWISE(Atable, Btable, Ctable, CTtable, BScanIteratorPriority,
        multOp, multOpOptions, plusOp, rowFilter, colFilterAT, colFilterB,
        true, true, iteratorsBeforeA,
        iteratorsBeforeB, iteratorsAfterTwoTable,
        reducer, reducerOpts,
        numEntriesCheckpoint, trace);
  }

  /**
   * C += A * B.
   * User-defined "plus" and "multiply". Requires transpose table AT instead of A.
   * If C is not given, then the scan itself returns the results of A * B. (not thoroughly tested)
   * @param ATtable              Name of Accumulo table holding matrix transpose(A).
   * @param Btable               Name of Accumulo table holding matrix B.
   * @param Ctable               Name of table to store result. Null means don't store the result.
   * @param CTtable              Name of table to store transpose of result. Null means don't store the transpose.
   * @param BScanIteratorPriority Priority to use for Table Multiplication scan-time iterator on table B
   * @param multOp               An operation that "multiplies" two values.
   * @param multOpOptions        Options for multiply ops that need configuring. Can be null if no options needed.
   * @param plusOp               An SKVI to apply to the result table that "sums" values. Not applied if null.
   * @param rowFilter            Row subset of ATtable and Btable, like "a,:,b,g,c,:,". Null means run on all rows.
   * @param colFilterAT          Column qualifier subset of AT. Null means run on all columns.
   * @param colFilterB           Column qualifier subset of B. Null means run on all columns.
   * @param alsoDoAA             Whether to also compute A*A at the same time as A*B. Default false.
   * @param alsoDoBB             Whether to also compute B*B at the same time as A*B. Default false.
   * @param numEntriesCheckpoint # of entries before we emit a checkpoint entry from the scan. -1 means no monitoring.
   * @param trace                Enable server-side performance tracing.
   * @return Number of partial products processed through the RemoteWriteIterator.
   */
  public long TableMult(String ATtable, String Btable, String Ctable, String CTtable,
                        int BScanIteratorPriority,
                        Class<? extends MultiplyOp> multOp, Map<String, String> multOpOptions,
                        IteratorSetting plusOp,
                        Collection<Range> rowFilter,
                        String colFilterAT, String colFilterB,
                        boolean alsoDoAA, boolean alsoDoBB,
                        int numEntriesCheckpoint, boolean trace) {
    return TwoTableROWCartesian(ATtable, Btable, Ctable, CTtable, BScanIteratorPriority,
        multOp, multOpOptions, plusOp, rowFilter, colFilterAT, colFilterB,
        alsoDoAA, alsoDoBB, alsoDoAA, alsoDoBB, Collections.<IteratorSetting>emptyList(),
        Collections.<IteratorSetting>emptyList(), Collections.<IteratorSetting>emptyList(),
        null, null,
        numEntriesCheckpoint, trace);
  }

  /**
   * C += A * B.
   * User-defined "plus" and "multiply". Requires transpose table AT instead of A.
   * If C is not given, then the scan itself returns the results of A * B. (not thoroughly tested)
   * @param ATtable              Name of Accumulo table holding matrix transpose(A).
   * @param Btable               Name of Accumulo table holding matrix B.
   * @param Ctable               Name of table to store result. Null means don't store the result.
   * @param CTtable              Name of table to store transpose of result. Null means don't store the transpose.
   * @param BScanIteratorPriority Priority to use for Table Multiplication scan-time iterator on table B
   * @param multOp               An operation that "multiplies" two values.
   * @param multOpOptions        Options for multiply ops that need configuring. Can be null if no options needed.
   * @param plusOp               An SKVI to apply to the result table that "sums" values. Not applied if null.
   * @param rowFilter            Row subset of ATtable and Btable, like "a,:,b,g,c,:,". Null means run on all rows.
   * @param colFilterAT          Column qualifier subset of AT. Null means run on all columns.
   * @param colFilterB           Column qualifier subset of B. Null means run on all columns.
   * @param alsoDoAA             Whether to also compute A*A at the same time as A*B. Default false.
   * @param alsoDoBB             Whether to also compute B*B at the same time as A*B. Default false.
   * @param iteratorsBeforeA     Extra iterators used on ATtable before TableMult.
   * @param iteratorsBeforeB     Extra iterators used on  Btable before TableMult.
   * @param iteratorsAfterTwoTable  Extra iterators used after TableMult but before writing entries to Ctable and CTtable.
   * @param reducer              Reducer used during operation. Null means no reducer. If not null, must already be init'ed.
   * @param reducerOpts          Options for the reducer; not used if reducer is null.
   * @param numEntriesCheckpoint # of entries before we emit a checkpoint entry from the scan. -1 means no monitoring.
   * @param trace                Enable server-side performance tracing.            @return Number of partial products processed through the RemoteWriteIterator.
   */
  public long TableMult(String ATtable, String Btable, String Ctable, String CTtable,
                        int BScanIteratorPriority,
                        Class<? extends MultiplyOp> multOp, Map<String, String> multOpOptions,
                        IteratorSetting plusOp,
                        Collection<Range> rowFilter, String colFilterAT, String colFilterB,
                        boolean alsoDoAA, boolean alsoDoBB,
                        List<IteratorSetting> iteratorsBeforeA, List<IteratorSetting> iteratorsBeforeB,
                        List<IteratorSetting> iteratorsAfterTwoTable,
                        Reducer reducer, Map<String,String> reducerOpts,
                        int numEntriesCheckpoint, boolean trace) {
    return TwoTableROWCartesian(ATtable, Btable, Ctable, CTtable, BScanIteratorPriority,
        multOp, multOpOptions, plusOp, rowFilter, colFilterAT, colFilterB,
        alsoDoAA, alsoDoBB, alsoDoAA, alsoDoBB, iteratorsBeforeA, iteratorsBeforeB, iteratorsAfterTwoTable,
        reducer, reducerOpts,
        numEntriesCheckpoint, trace);
  }

  public long TwoTableROWCartesian(String ATtable, String Btable, String Ctable, String CTtable,
                                   int BScanIteratorPriority,
                                   //TwoTableIterator.DOTMODE dotmode, //CartesianRowMultiply.ROWMODE rowmode,
                                   Class<? extends MultiplyOp> multOp, Map<String, String> multOpOptions,
                                   IteratorSetting plusOp,
                                   Collection<Range> rowFilter,
                                   String colFilterAT, String colFilterB,
                                   boolean emitNoMatchA, boolean emitNoMatchB,
                                   boolean alsoDoAA, boolean alsoDoBB,
                                   List<IteratorSetting> iteratorsBeforeA, List<IteratorSetting> iteratorsBeforeB,
                                   List<IteratorSetting> iteratorsAfterTwoTable,
                                   Reducer reducer, Map<String,String> reducerOpts,
                                   int numEntriesCheckpoint, boolean trace) {
    if (multOp == null)
      multOp = MathTwoScalar.class;
    Map<String,String> opt = new HashMap<String, String>();
    opt.put("rowMultiplyOp", CartesianRowMultiply.class.getName());
    opt.put("rowMultiplyOp.opt.multiplyOp", multOp.getName()); // treated same as multiplyOp
    if (multOpOptions != null)
      for (Map.Entry<String, String> entry : multOpOptions.entrySet()) {
        opt.put("multiplyOp.opt."+entry.getKey(), entry.getValue()); // treated same as multiplyOp
      }
    opt.put("rowMultiplyOp.opt.rowmode", CartesianRowMultiply.ROWMODE.ONEROWA.name());
    opt.put("rowMultiplyOp.opt."+CartesianRowMultiply.ALSODOAA, Boolean.toString(alsoDoAA));
    opt.put("rowMultiplyOp.opt."+CartesianRowMultiply.ALSODOBB, Boolean.toString(alsoDoBB));

    return TwoTable(ATtable, Btable, Ctable, CTtable, BScanIteratorPriority,
        TwoTableIterator.DOTMODE.ROW, opt, plusOp,
        rowFilter, colFilterAT, colFilterB,
        emitNoMatchA, emitNoMatchB, iteratorsBeforeA, iteratorsBeforeB, iteratorsAfterTwoTable,
        reducer, reducerOpts,
        numEntriesCheckpoint, trace);
  }

  public long TwoTableROWSelector(
      String ATtable, String Btable, String Ctable, String CTtable,
      int BScanIteratorPriority,
      Collection<Range> rowFilter,
      String colFilterAT, String colFilterB,
      boolean ASelectsBRow,
      List<IteratorSetting> iteratorsBeforeA, List<IteratorSetting> iteratorsBeforeB,
      List<IteratorSetting> iteratorsAfterTwoTable,
      Reducer reducer, Map<String,String> reducerOpts,
      int numEntriesCheckpoint, boolean trace
  ) {
    Map<String,String> opt = new HashMap<String, String>();
    opt.put("rowMultiplyOp", SelectorRowMultiply.class.getName());
    opt.put("rowMultiplyOp.opt."+SelectorRowMultiply.ASELECTSBROW, Boolean.toString(ASelectsBRow));

    return TwoTable(ATtable, Btable, Ctable, CTtable, BScanIteratorPriority,
        TwoTableIterator.DOTMODE.ROW, opt, null,
        rowFilter, colFilterAT, colFilterB,
        false, false, iteratorsBeforeA, iteratorsBeforeB, iteratorsAfterTwoTable,
        reducer, reducerOpts,
        numEntriesCheckpoint, trace);
  }

  public long TwoTableEWISE(String ATtable, String Btable, String Ctable, String CTtable,
                            int BScanIteratorPriority,
                            //TwoTableIterator.DOTMODE dotmode, //CartesianRowMultiply.ROWMODE rowmode,
                            Class<? extends EWiseOp> multOp, Map<String, String> multOpOptions,
                            IteratorSetting plusOp,
                            Collection<Range> rowFilter,
                            String colFilterAT, String colFilterB,
                            boolean emitNoMatchA, boolean emitNoMatchB,
                            List<IteratorSetting> iteratorsBeforeA, List<IteratorSetting> iteratorsBeforeB,
                            List<IteratorSetting> iteratorsAfterTwoTable,
                            Reducer reducer, Map<String,String> reducerOpts,
                            int numEntriesCheckpoint, boolean trace) {
    if (multOp == null)
      multOp = MathTwoScalar.class;
    Map<String,String> opt = new HashMap<String, String>();
    opt.put("multiplyOp", multOp.getName());
    if (multOpOptions != null)
      for (Map.Entry<String, String> entry : multOpOptions.entrySet()) {
        opt.put("multiplyOp.opt."+entry.getKey(), entry.getValue()); // treated same as multiplyOp
      }

    return TwoTable(ATtable, Btable, Ctable, CTtable, BScanIteratorPriority,
        TwoTableIterator.DOTMODE.EWISE, opt, plusOp,
        rowFilter, colFilterAT, colFilterB,
        emitNoMatchA, emitNoMatchB, iteratorsBeforeA, iteratorsBeforeB, iteratorsAfterTwoTable,
        reducer, reducerOpts,
        numEntriesCheckpoint, trace);
  }

  public long TwoTableNONE(String ATtable, String Btable, String Ctable, String CTtable,
                           int BScanIteratorPriority,
                           //TwoTableIterator.DOTMODE dotmode, //CartesianRowMultiply.ROWMODE rowmode,
                           IteratorSetting plusOp,
                           Collection<Range> rowFilter,
                           String colFilterAT, String colFilterB,
                           boolean emitNoMatchA, boolean emitNoMatchB,
                           List<IteratorSetting> iteratorsBeforeA, List<IteratorSetting> iteratorsBeforeB,
                           List<IteratorSetting> iteratorsAfterTwoTable,
                           Reducer reducer, Map<String,String> reducerOpts,
                           int numEntriesCheckpoint, boolean trace) {
    Map<String,String> opt = new HashMap<String, String>();

    return TwoTable(ATtable, Btable, Ctable, CTtable, BScanIteratorPriority,
        TwoTableIterator.DOTMODE.NONE, opt, plusOp,
        rowFilter, colFilterAT, colFilterB,
        emitNoMatchA, emitNoMatchB, iteratorsBeforeA, iteratorsBeforeB, iteratorsAfterTwoTable,
        reducer, reducerOpts,
        numEntriesCheckpoint, trace);
  }

  public long TwoTable(String ATtable, String Btable, String Ctable, String CTtable,
                       int BScanIteratorPriority,
                       TwoTableIterator.DOTMODE dotmode, Map<String, String> optsTT,
                       IteratorSetting plusOp, // priority matters
                       Collection<Range> rowFilter,
                       String colFilterAT, String colFilterB,
                       boolean emitNoMatchA, boolean emitNoMatchB,
                       // RemoteSourceIterator has its own priority for scan-time iterators.
                       // Could override by "diterPriority" option
                       List<IteratorSetting> iteratorsBeforeA, List<IteratorSetting> iteratorsBeforeB,
                       List<IteratorSetting> iteratorsAfterTwoTable, // priority doesn't matter for these three
                       Reducer reducer, Map<String,String> reducerOpts, // applies at RWI if using RWI; otherwise applies at client. Reducer must be init'ed previously
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
    if (iteratorsAfterTwoTable == null) iteratorsAfterTwoTable = Collections.emptyList();
    if (iteratorsBeforeA == null) iteratorsBeforeA = Collections.emptyList();
    if (iteratorsBeforeB == null) iteratorsBeforeB = Collections.emptyList();
    if (BScanIteratorPriority <= 0)
      BScanIteratorPriority = 7; // default priority
    if (reducerOpts == null && reducer != null)
      reducerOpts = new HashMap<String, String>();

    Ctable = emptyToNull(Ctable);
    CTtable = emptyToNull(CTtable);
    boolean useRWI = Ctable != null || CTtable != null || reducer != null;
    if (!useRWI)
      log.warn("Streaming back result of multiplication to client does not guarantee correctness." +
          "In particular, if Accumulo destroys, re-inits and re-seeks an iterator stack, the stack may not recover.");


    if (dotmode == null)
      throw new IllegalArgumentException("dotmode is required but given null");
//    if (multOp == null)
//      throw new IllegalArgumentException("multOp is required but given null");
    if (rowFilter != null && (rowFilter.isEmpty() || rowFilter.contains(new Range())))
      rowFilter = null;

    TableOperations tops = connector.tableOperations();
    if (!ATtable.equals(TwoTableIterator.CLONESOURCE_TABLENAME) && !tops.exists(ATtable))
      throw new IllegalArgumentException("Table AT does not exist. Given: " + ATtable);
    if (!tops.exists(Btable))
      throw new IllegalArgumentException("Table B does not exist. Given: " + Btable);

    if (Ctable != null && !tops.exists(Ctable))
      try {
        tops.create(Ctable);
      } catch (AccumuloException e) {
        log.error("error trying to create Ctable " + Ctable, e);
        throw new RuntimeException(e);
      } catch (AccumuloSecurityException e) {
        log.error("error trying to create Ctable " + Ctable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }

    if (CTtable != null && !tops.exists(CTtable))
      try {
        tops.create(CTtable);
      } catch (AccumuloException e) {
        log.error("error trying to create CTtable " + Ctable, e);
        throw new RuntimeException(e);
      } catch (AccumuloSecurityException e) {
        log.error("error trying to create CTtable " + Ctable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }

    Map<String, String>
        optTT = basicRemoteOpts("AT.", ATtable),
        optRWI = (useRWI) ? basicRemoteOpts("", Ctable, CTtable) : null;
    optTT.put("trace", String.valueOf(trace)); // logs timing on server
    optTT.put("dotmode", dotmode.name());
    optTT.putAll(optsTT);
    if (colFilterAT != null)
      optTT.put("AT.colFilter", colFilterAT);
    if (useRWI && numEntriesCheckpoint > 0)
      optRWI.put("numEntriesCheckpoint", String.valueOf(numEntriesCheckpoint));
    optTT.put("AT.emitNoMatch", Boolean.toString(emitNoMatchA));
    optTT.put("B.emitNoMatch", Boolean.toString(emitNoMatchB));

    if (Ctable != null && plusOp != null)
      GraphuloUtil.applyIteratorSoft(plusOp, tops, Ctable);
    if (CTtable != null && plusOp != null)
      GraphuloUtil.applyIteratorSoft(plusOp, tops, CTtable);

    if (reducer != null) {
      optRWI.put("reducer", reducer.getClass().getName());
      for (Map.Entry<String, String> entry : reducerOpts.entrySet()) {
        optRWI.put("reducer.opt."+entry.getKey(), entry.getValue());
      }
    }

    // scan B with TableMultIterator
    BatchScanner bs;
    try {
      bs = connector.createBatchScanner(Btable, Authorizations.EMPTY, 50); // TODO P2: set number of batch scan threads
    } catch (TableNotFoundException e) {
      log.error("crazy", e);
      throw new RuntimeException(e);
    }

    if (rowFilter != null) {
      if (useRWI) {
        optRWI.put("rowRanges", GraphuloUtil.rangesToD4MString(rowFilter)); // translate row filter to D4M notation
        bs.setRanges(Collections.singleton(new Range()));
      } else
        bs.setRanges(rowFilter);
    } else
      bs.setRanges(Collections.singleton(new Range()));

    DynamicIteratorSetting dis = new DynamicIteratorSetting();
    for (IteratorSetting setting : iteratorsBeforeA)
      dis.append(setting);
    optTT.putAll(dis.buildSettingMap("AT.diter."));

    dis = new DynamicIteratorSetting();
    for (IteratorSetting setting : iteratorsBeforeB)
      dis.append(setting);
    optTT.putAll(dis.buildSettingMap("B.diter."));

    dis = new DynamicIteratorSetting();
    GraphuloUtil.applyGeneralColumnFilter(colFilterB, bs, dis, true);
    dis.append(new IteratorSetting(1, TwoTableIterator.class, optTT));
    for (IteratorSetting setting : iteratorsAfterTwoTable)
      dis.append(setting);
//    dis.append(new IteratorSetting(1, DebugInfoIterator.class)); // DEBUG
    if (useRWI)
      dis.append(new IteratorSetting(1, RemoteWriteIterator.class, optRWI));
    bs.addScanIterator(dis.toIteratorSetting(BScanIteratorPriority));



    // for monitoring: reduce table-level parameter controlling when Accumulo sends back an entry to the client
    String prevPropTableScanMaxMem = null;
    if (numEntriesCheckpoint > 0 && useRWI)
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
      } catch (AccumuloException e) {
        log.warn("error trying to get and set property table.scan.max.memory for " + Btable, e);
      } catch (AccumuloSecurityException e) {
        log.warn("error trying to get and set property table.scan.max.memory for " + Btable, e);
      } catch (TableNotFoundException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }

    // Do the BatchScan on B
    long numEntries = 0, thisEntries;
    try {
      for (Map.Entry<Key, Value> entry : bs) {
        if (useRWI) {
//          log.debug(entry.getKey() + " -> " + entry.getValue() + " AS " + Key.toPrintableString(entry.getValue().get(), 0, entry.getValue().get().length, 40) + " RAW "+ Arrays.toString(entry.getValue().get()));
          thisEntries = RemoteWriteIterator.decodeValue(entry.getValue(), reducer);
          log.debug(entry.getKey() + " -> " + thisEntries + " entries processed");
          numEntries += thisEntries;
        } else {
          log.debug(entry.getKey() + " -> " + entry.getValue());
        }
      }
    } finally {
      bs.close();
      if (prevPropTableScanMaxMem != null) {
        try {
          tops.setProperty(Btable, "table.scan.max.memory", prevPropTableScanMaxMem);
        } catch (AccumuloException e) {
          log.error("cannot reset table.scan.max.memory property for " + Btable + " to " + prevPropTableScanMaxMem, e);
        } catch (AccumuloSecurityException e) {
          log.error("cannot reset table.scan.max.memory property for " + Btable + " to " + prevPropTableScanMaxMem, e);
        }
      }
    }

    return numEntries;

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



  // reducer, if not null, must be already init'ed.

  /**
   * One-table operation.
   * Includes the boilerplate necessary to filter, apply iterators, apply a Reducer,
   * and write to result tables.
   *
   * @param Atable Input table to scan over. Must already exist.
   * @param Rtable Output table to write result entries to.  Can be null.
   * @param RTtable Output table to write transpose of result entries to.  Can be null.
   * @param clientResultMap Only give non-null if Rtable==null && RTtable==null.
   *                        Results of BFS are put inside the map instead of into a new Accumulo table.
   *                        Yes, this means all the entries reached in the BFS are sent to the client. Use with care.
   *                        Post-condition: this map will be modified, adding entries reached.
   * @param AScanIteratorPriority Priority to use for scan-time iterator on table A
   * @param reducer Reducer to gather information from entries passing through the RemoteWriteIterator. Can be null.
   *                If not null, then it must have already been init'ed.
   *                Post-condition: the reducer will be modified to include information from all traversed entries.
   * @param reducerOpts Options for the reducer. Only give if reducer is given too.
   * @param plusOp An SKVI to apply to the result table(s) that "sums" values. Not applied if null.
   *               Applied at the client if Rtable==null && RTtable==null && clientResultMap != null.
   * @param rowFilter Only reads rows in the given Ranges. Null means all rows.
   * @param colFilter Only acts on entries with a matching column. This is interpreted as a D4M string.
   *                   Null means all columns. Note that the columns are still read, just not sent through the iterator stack.
   * @param midIterator Iterators to apply after the row and column filtering but before the RemoteWriteIterator.
   *                     Always applied at the server.
   * @param bs Pass in a BatchScanner if you wish to re-use a BatchScanner across multiple calls.
   *           If given, will NOT call close().  If not given, will create a new one and close it before finishing.
   *           Post-condition: This method will <b>CLEAR scan-time iterators and fetched columns</b> from the BatchScanner.
   *           Thus, scan-time iterators and fetched columns on a given BatchScanner will affect this OneTable operation once.
   *           It is better to specify a column filter and midIterator instead of setting these on the BatchScanner directly.
   * @param trace     Enable server-side performance tracing.
   * @return Number of entries processed at the RemoteWriteIterator or gathered at the client.
   *
   */
  public long OneTable(String Atable, String Rtable, String RTtable,      // Input, output table names
                       Map<Key, Value> clientResultMap,                   // controls whether to use RWI
                       int AScanIteratorPriority,                         // Scan-time iterator priority
                       Reducer reducer, Map<String, String> reducerOpts,  // Applies at RemoteWriteIterator and/or client
                       IteratorSetting plusOp,                            // priority matters
                       Collection<Range> rowFilter,
                       String colFilter,
                       List<IteratorSetting> midIterator,                 // Applied after row, col filter but before RWI
                       BatchScanner bs,                                   // Optimization: re-use BatchScanner
                       boolean trace) {
    boolean useRWI = clientResultMap == null;
    if (Atable == null || Atable.isEmpty())
      throw new IllegalArgumentException("Please specify table A. Given: " + Atable);
    // Prevent possibility for infinite loop:
    if (Atable.equals(Rtable))
      throw new UnsupportedOperationException("Could lead to unpredictable results: Atable=Rtable=" + Atable);
    if (Atable.equals(RTtable))
      throw new UnsupportedOperationException("Could lead to unpredictable results: Atable=RTtable=" + Atable);
    if (midIterator == null) midIterator = Collections.emptyList();
    if (AScanIteratorPriority <= 0)
      AScanIteratorPriority = 27; // default priority
    if (reducerOpts == null)
      reducerOpts = new HashMap<String, String>();

    Rtable = emptyToNull(Rtable);
    RTtable = emptyToNull(RTtable);
    Preconditions.checkArgument(useRWI || (Rtable == null && RTtable == null),
        "clientResultMap must be null if given an Rtable or RTtable");
//    if (!useRWI) {
//      log.warn("Experimental: Streaming back result of multiplication to client." +
//          "If Accumulo destroys, re-inits and re-seeks an iterator stack, the stack may not recover.");
//    }
    if (rowFilter != null && (rowFilter.isEmpty() || rowFilter.contains(new Range())))
      rowFilter = null;

    TableOperations tops = connector.tableOperations();
    if (!tops.exists(Atable))
      throw new IllegalArgumentException("Table A does not exist. Given: " + Atable);

    // special case worth optimizing; we can clone if minimal options present
    // Returns -1 if number of entries cannot be determined, e.g., because a table clone is used as an optimization.
    // Chose not to include this so that OneTable would always return the partial product count (less confusion).
    /*if (useRWI && rowFilter == null && colFilter == null && plusOp == null && reducer == null
            && midIterator.isEmpty() && Rtable != null && !tops.exists(Rtable) && RTtable == null) {
      try {
          tops.clone(Atable, Rtable, true, null, null);
      } catch (AccumuloException | AccumuloSecurityException e) {
          log.error("trouble cloning "+Atable+" to "+Rtable, e);
          throw new RuntimeException(e);
      } catch (TableNotFoundException | TableExistsException e) {
          log.error("crazy",e);
          throw new RuntimeException(e);
      }
      return -1;
    }*/

    if (Rtable != null && !tops.exists(Rtable))
      try {
        tops.create(Rtable);
      } catch (AccumuloException e) {
        log.error("error trying to create Rtable " + Rtable, e);
        throw new RuntimeException(e);
      } catch (AccumuloSecurityException e) {
        log.error("error trying to create Rtable " + Rtable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }

    if (RTtable != null && !tops.exists(RTtable))
      try {
        tops.create(RTtable);
      } catch (AccumuloException e) {
        log.error("error trying to create RTtable " + Rtable, e);
        throw new RuntimeException(e);
      } catch (AccumuloSecurityException e) {
        log.error("error trying to create RTtable " + Rtable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }

    if (Rtable != null && plusOp != null)
      GraphuloUtil.applyIteratorSoft(plusOp, tops, Rtable);
    if (RTtable != null && plusOp != null)
      GraphuloUtil.applyIteratorSoft(plusOp, tops, RTtable);

    boolean givenBS = bs != null;
    if (bs == null)
      try {
        bs = connector.createBatchScanner(Atable, Authorizations.EMPTY, 50); // TODO P2: set number of batch scan threads
      } catch (TableNotFoundException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }
    bs.setRanges(Collections.singleton(new Range()));

    Map<String, String>
        optRWI = useRWI ? basicRemoteOpts("", Rtable, RTtable) : null;
    if (useRWI)
      optRWI.put("trace", String.valueOf(trace)); // logs timing on server

    DynamicIteratorSetting dis = new DynamicIteratorSetting();

    if (rowFilter != null) {
      Map<String,String> rowFilterOpt = Collections.singletonMap("rowRanges", GraphuloUtil.rangesToD4MString(rowFilter));
      if (useRWI)
        optRWI.put("rowRanges", GraphuloUtil.rangesToD4MString(rowFilter)); // translate row filter to D4M notation
      else
        dis.append(new IteratorSetting(4, SeekFilterIterator.class, rowFilterOpt));
    }

    if (colFilter != null)
      GraphuloUtil.applyGeneralColumnFilter(colFilter, bs, dis, true);

    for (IteratorSetting setting : midIterator)
      dis.append(setting);

    if (useRWI && reducer != null) {
      optRWI.put("reducer", reducer.getClass().getName());
      for (Map.Entry<String, String> entry : reducerOpts.entrySet()) {
        optRWI.put("reducer.opt."+entry.getKey(), entry.getValue());
      }
    }
    if (useRWI)
      dis.append(new IteratorSetting(1, RemoteWriteIterator.class, optRWI));

    dis.addToScanner(bs, AScanIteratorPriority);

    long numEntries = 0, thisEntries;
    try {
      BatchScanner THISBS = bs;
      if (!useRWI) {
        THISBS = new ClientSideIteratorAggregatingScanner(THISBS);
        if (plusOp != null)
          THISBS.addScanIterator(plusOp);
      }

      for (Map.Entry<Key, Value> entry : THISBS) {
        if (useRWI) {
//          log.debug(entry.getKey() + " -> " + entry.getValue() + " AS " + Key.toPrintableString(entry.getValue().get(), 0, entry.getValue().get().length, 40) + " RAW "+ Arrays.toString(entry.getValue().get()));
          // mutates reducer if not null:
          thisEntries = RemoteWriteIterator.decodeValue(entry.getValue(), reducer);
          log.debug(entry.getKey() + " -> " + thisEntries + " entries processed");
          numEntries += thisEntries;
        } else {
//          log.debug(entry.getKey() + " -> " + entry.getValue());
          clientResultMap.put(entry.getKey(), entry.getValue());
          if (reducer != null)
            reducer.update(entry.getKey(), entry.getValue());
          numEntries++;
        }
      }
    } finally {
      if (!givenBS)
        bs.close();
      else {
        bs.clearScanIterators();
        bs.clearColumns();
      }
    }

    return numEntries;
  }


  /**
   * Use LongCombiner to sum.
   */
  public String AdjBFS(String Atable, String v0, int k, String Rtable, String RtableTranspose,
                       String ADegtable, String degColumn, boolean degInColQ, int minDegree, int maxDegree) {

    return AdjBFS(Atable, v0, k, Rtable, RtableTranspose, null, -1,
        ADegtable, degColumn, degInColQ, minDegree, maxDegree,
            PLUS_ITERATOR_BIGDECIMAL, false);
  }

  /**
   * Adjacency table Breadth First Search. Sums entries into Rtable from each step of the BFS.
   * Can gather entries at client if desired.
   *
   * @param Atable      Name of Accumulo table.
   * @param v0          Starting nodes, like "a,f,b,c,". Null or empty string "" means start from all nodes.
   *                    v0 may be a range of nodes like "c,:,e,g,k,:,".
   * @param k           Number of steps
   * @param Rtable      Name of table to store result. Null means don't store the result.
   * @param RTtable     Name of table to store transpose of result. Null means don't store the transpose.
   * @param clientResultMap Only give non-null if Rtable==null && RTtable==null.
   *                        Results of BFS are put inside the map instead of into a new Accumulo table.
   *                        Yes, this means all the entries reached in the BFS are sent to the client. Use with care.
   * @param AScanIteratorPriority Priority to use for scan-time iterator on table A
   * @param ADegtable   Name of table holding out-degrees for A. Leave null to filter on the fly with
 *                    the {@link SmallLargeRowFilter}, or do no filtering if minDegree=0 and maxDegree=Integer.MAX_VALUE.
   * @param degColumn   Name of column for out-degrees in ADegtable like "deg". Null means the empty column "".
*                    If degInColQ==true, this is the prefix before the numeric portion of the column like "deg|", and null means no prefix. Unused if ADegtable is null.
   * @param degInColQ   True means degree is in the Column Qualifier. False means degree is in the Value.
*                    Unused if ADegtable is null.
   * @param minDegree   Minimum out-degree. Checked before doing any searching, at every step, from ADegtable. Pass 0 for no filtering.
   * @param maxDegree   Maximum out-degree. Checked before doing any searching, at every step, from ADegtable. Pass Integer.MAX_VALUE for no filtering.
   * @param plusOp      An SKVI to apply to the result table(s) that "sums" values. Not applied if null.
   *                    Applied at the client if Rtable==null && RTtable==null && clientResultMap != null.
   * @param trace       Enable server-side performance tracing.
   * @return          The nodes reachable in exactly k steps from v0.
   */
  @SuppressWarnings("unchecked")
  public String AdjBFS(String Atable, String v0, int k, String Rtable, String RTtable,
                       Map<Key, Value> clientResultMap, int AScanIteratorPriority,
                       String ADegtable, String degColumn, boolean degInColQ, int minDegree, int maxDegree,
                       IteratorSetting plusOp,
                       boolean trace) {
    boolean needDegreeFiltering = minDegree > 1 || maxDegree < Integer.MAX_VALUE;
    boolean useRWI = clientResultMap == null;
    checkGiven(true, "Atable", Atable);
    if (minDegree < 1)
      minDegree = 1;
    ADegtable = emptyToNull(ADegtable);
    Rtable = emptyToNull(Rtable);
    RTtable = emptyToNull(RTtable);
    Preconditions.checkArgument(maxDegree >= minDegree, "maxDegree=%s should be >= minDegree=%s", maxDegree, minDegree);
    if (AScanIteratorPriority <= 0)
      AScanIteratorPriority = 4; // default priority

    Preconditions.checkArgument(useRWI || (Rtable == null && RTtable == null),
        "clientResultMap must be null if given an Rtable or RTtable");
//    Preconditions.checkArgument(Rtable != null || RTtable != null || clientResultMap != null,
//        "For writing results to an Accumulo table, please set Rtable != null || RTtable != null." +
//            "For gathering results at the client, please set clientResultMap != null.");

    if (degColumn == null)
      degColumn = "";
    Text degColumnText = new Text(degColumn);

    if (v0 == null || v0.isEmpty())
      v0 = ":"+GraphuloUtil.DEFAULT_SEP_D4M_STRING;
    Collection<Text> vktexts = new HashSet<Text>(); //v0 == null ? null : GraphuloUtil.d4mRowToTexts(v0);
    char sep = v0.charAt(v0.length() - 1);

    BatchScanner bs, bsDegree = null;
    try {
      bs = connector.createBatchScanner(Atable, Authorizations.EMPTY, 50); // TODO P2: set number of batch scan threads
      if (needDegreeFiltering & ADegtable != null)
        bsDegree = connector.createBatchScanner(ADegtable, Authorizations.EMPTY, 4); // TODO P2: set number of batch scan threads
    } catch (TableNotFoundException e) {
      log.error("crazy", e);
      throw new RuntimeException(e);
    }

    List<IteratorSetting> iteratorSettingList = new ArrayList<IteratorSetting>();

    IteratorSetting itsetDegreeFilter = null;
    if (needDegreeFiltering && ADegtable == null)
      itsetDegreeFilter = SmallLargeRowFilter.iteratorSetting(3, minDegree, maxDegree);

    try {
      long degTime = 0, scanTime = 0;
      for (int thisk = 1; thisk <= k; thisk++) {
        if (trace)
          if (thisk == 1)
            System.out.println("First step: v0 is " + v0);
          else
            System.out.println("k=" + thisk + " before filter" +
                (vktexts.size() > 5 ? " #=" + String.valueOf(vktexts.size()) : ": " + vktexts.toString()));

        iteratorSettingList.clear();
        Collection<Range> rowFilter;

        if (needDegreeFiltering && ADegtable != null) { // use degree table
          long t1 = System.currentTimeMillis(), dur;
          vktexts = thisk == 1
              ? filterTextsDegreeTable(bsDegree, degColumnText, degInColQ, minDegree, maxDegree, vktexts, GraphuloUtil.d4mRowToRanges(v0))
              : filterTextsDegreeTable(bsDegree, degColumnText, degInColQ, minDegree, maxDegree, vktexts);
          dur = System.currentTimeMillis() - t1;
          degTime += dur;
          if (trace) {
            System.out.println("Degree Lookup Time: " + dur + " ms");
            System.out.println("k=" + thisk + " after  filter" +
                (vktexts.size() > 5 ? " #=" + String.valueOf(vktexts.size()) : ": " + vktexts.toString()));
          }

          if (vktexts.isEmpty())
            break;
//          opt.put("rowRanges", GraphuloUtil.textsToD4mString(vktexts, sep));
          rowFilter = GraphuloUtil.textsToRanges(vktexts);

        } else {  // no degree table or no filtering
          if (thisk == 1)
//            opt.put("rowRanges", v0);
            rowFilter = GraphuloUtil.d4mRowToRanges(v0);
          else
//            opt.put("rowRanges", GraphuloUtil.textsToD4mString(vktexts, sep));
            rowFilter = GraphuloUtil.textsToRanges(vktexts);
          if (needDegreeFiltering) // filtering but no degree table
            iteratorSettingList.add(itsetDegreeFilter);
        }

        GatherColQReducer reducer = new GatherColQReducer();
        reducer.init(Collections.<String, String>emptyMap(), null);

        long t2 = System.currentTimeMillis(), dur;
        OneTable(Atable, Rtable, RTtable, clientResultMap, AScanIteratorPriority,
            reducer, Collections.<String, String>emptyMap(), plusOp,
            rowFilter, null, // no column filter
            iteratorSettingList, bs,
            trace);
        // post-condition: reducer updated; clientResultMap updated if not null

        dur = System.currentTimeMillis() - t2;
        scanTime += dur;

        vktexts.clear();
//      vktexts.addAll(reducer.getSerializableForClient());
        for (String uk : reducer.getSerializableForClient()) {
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
      if (bsDegree != null)
        bsDegree.close();
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
   * Re-uses a BatchScanner, which must be created for the degree table prior to this method. Does not close the BatchScanner.
   *
   * @param degColumnText   Name of the degree column qualifier. Blank/null means fetch the empty ("") column.
   * @param degInColQ False means degree in value. True means degree in column qualifier and that degColumnText is a prefix before the numeric portion of the column qualifier degree.
   * @return The same texts object, with nodes that fail the degree filter removed.
   */
  private Collection<Text> filterTextsDegreeTable(BatchScanner bs, Text degColumnText, boolean degInColQ,
                                                  int minDegree, int maxDegree,
                                                  Collection<Text> texts) {
    if (texts == null || texts.isEmpty())
      return texts;
    return filterTextsDegreeTable(bs, degColumnText, degInColQ,
        minDegree, maxDegree, texts, GraphuloUtil.textsToRanges(texts));
  }

  /** Used when thisk==1, on first call to BFS. */
  private Collection<Text> filterTextsDegreeTable(BatchScanner bs, Text degColumnText, boolean degInColQ,
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
    assert (minDegree > 1 || maxDegree < Integer.MAX_VALUE) && maxDegree >= minDegree;

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
        } catch (NumberFormatException e) {
          log.warn("Trouble parsing degree entry as long; assuming bad degree: " + entry.getKey() + " -> " + entry.getValue(), e);
          bad = true;
        } catch (ValueFormatException e) {
          log.warn("Trouble parsing degree entry as long; assuming bad degree: " + entry.getKey() + " -> " + entry.getValue(), e);
          bad = true;
        }

        if (!addToTexts && bad) {
          boolean remove = texts.remove(entry.getKey().getRow(badRow));
          if (!remove)
            log.warn("Unrecognized entry with bad degree; cannot remove: " + entry.getKey() + " -> " + entry.getValue());
        } else if (addToTexts && !bad) {
          texts.add(entry.getKey().getRow()); // need new Text object
        }
      }
    } finally {
      bs.setRanges(Collections.singletonList(new Range()));
      bs.clearColumns();
      bs.clearScanIterators();
    }
    return texts;
  }

  /**
   * Out-degree-filtered Breadth First Search on Incidence table.
   * Conceptually k iterations of: v0 ==startPrefixes==> edge ==endPrefixes==> v1.
   * Works for multi-edge search.
   *
   * @param Etable        Incidence table; rows are edges, column qualifiers are nodes.
   * @param v0            Starting vertices, like "v0,v5,v6,".
   *                      v0 may be a range of nodes like "c,:,e,g,k,:,".
   * @param k             Number of steps.
   * @param Rtable        Name of table to store result. Null means don't store the result.
   * @param RTtable       Name of table to store transpose of result. Null means don't store the transpose.
   * @param startPrefixes   D4M String of Prefixes of edge 'starts' including separator, e.g. 'outA|,outB|,'.
   *                        The "empty prefix" takes the form of ','. Required; if null or empty, assumes the empty prefix.
   * @param endPrefixes     D4M String of Prefixes of edge 'ends' including separator, e.g. 'inA|,inB|,'.
   *                        The "empty prefix" takes the form of ','. Required; if null or empty, assumes the empty prefix.
   *                        Important for multi-edge: None of the end prefixes may be a prefix of another end prefix.
   * @param ETDegtable    Name of table holding out-degrees for ET. Must be provided if degree filtering is used.
   * @param degColumn   Name of column for out-degrees in ETDegtable like "deg". Null means the empty column "".
   *                    If degInColQ==true, this is the prefix before the numeric portion of the column like "deg|", and null means no prefix.
   *                    Unused if ETDegtable is null.
   * @param degInColQ   True means degree is in the Column Qualifier. False means degree is in the Value. Unused if ETDegtable is null.
   * @param minDegree     Minimum out-degree. Checked before doing any searching, at every step, from ADegtable. Pass 0 for no filtering.
   * @param maxDegree     Maximum out-degree. Checked before doing any searching, at every step, from ADegtable. Pass Integer.MAX_VALUE for no filtering.
   * @param plusOp      An SKVI to apply to the result table that "sums" values. Not applied if null.
   * @param EScanIteratorPriority Priority to use for Table Multiplication scan-time iterator on table E
   * @param trace       Enable server-side performance tracing.
   * @return              The nodes reachable in exactly k steps from v0.
   */
  public String  EdgeBFS(String Etable, String v0, int k, String Rtable, String RTtable,
                         String startPrefixes, String endPrefixes,
                         String ETDegtable, String degColumn, boolean degInColQ, int minDegree, int maxDegree,
                         IteratorSetting plusOp, int EScanIteratorPriority, boolean trace) {
    boolean needDegreeFiltering = minDegree > 1 || maxDegree < Integer.MAX_VALUE;
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
    if (EScanIteratorPriority <= 0)
      EScanIteratorPriority = 5; // default priority

    if (degColumn == null)
      degColumn = "";
    Text degColumnText = new Text(degColumn);
    if (plusOp != null && plusOp.getPriority() >= 20)
      log.warn("Sum iterator setting is >=20. Are you sure you want the priority after the default Versioning iterator priority? " + plusOp);
    if (v0 == null || v0.isEmpty())
      v0 = ":\t";
    Collection<Text> vktexts = new HashSet<Text>();
    char sep = v0.charAt(v0.length() - 1);

    if (startPrefixes == null || startPrefixes.isEmpty())
      startPrefixes = Character.toString(sep);
    if (endPrefixes == null || endPrefixes.isEmpty())
      endPrefixes = Character.toString(sep);
    // error if any endPrefix is a prefix of another
    {
      String[] eps = GraphuloUtil.splitD4mString(endPrefixes);
      for (int i = 0; i < eps.length-1; i++) {
        for (int j = i+1; j < eps.length; j++)
          Preconditions.checkArgument(!eps[i].startsWith(eps[j]) && !eps[j].startsWith(eps[i]),
              "No end prefix should can be a prefix of another. Reason is that these endPrefixes conflict; " +
                  "no way to tell what node comes after the shorter end prefix. Two conflicting end prefixes: %s %s",
              eps[i], eps[j]);
      }
    }


    TableOperations tops = connector.tableOperations();
    if (!tops.exists(Etable))
      throw new IllegalArgumentException("Table E does not exist. Given: " + Etable);
    if (Rtable != null && !tops.exists(Rtable))
      try {
        tops.create(Rtable);
      } catch (AccumuloException e) {
        log.error("error trying to create R table " + Rtable, e);
        throw new RuntimeException(e);
      } catch (AccumuloSecurityException e) {
        log.error("error trying to create R table " + Rtable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }
    if (RTtable != null && !tops.exists(RTtable))
      try {
        tops.create(RTtable);
      } catch (AccumuloException e) {
        log.error("error trying to create R table transpose " + RTtable, e);
        throw new RuntimeException(e);
      } catch (AccumuloSecurityException e) {
        log.error("error trying to create R table transpose " + RTtable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }

    Map<String, String> opt = Rtable != null || RTtable != null ? basicRemoteOpts("C.", Rtable, RTtable) : new HashMap<String,String>();
//    opt.put("trace", String.valueOf(trace)); // logs timing on server
//    opt.put("gatherColQs", "true");  No gathering right now.  Need to implement more general gathering function on RemoteWriteIterator.
    opt.put("dotmode", TwoTableIterator.DOTMODE.ROW.name());
    opt.put("multiplyOp", EdgeBFSMultiply.class.getName());
//    opt.put("AT.zookeeperHost", zookeepers);
//    opt.put("AT.instanceName", instance);
    opt.put("AT.tableName", TwoTableIterator.CLONESOURCE_TABLENAME);
//    opt.put("AT.username", user);
//    opt.put("AT.password", new String(password.getPassword()));


    if (Rtable != null || RTtable != null) {
      opt.put("C.reducer", EdgeBFSReducer.class.getName());
      opt.put("C.reducer.opt."+EdgeBFSReducer.IN_COLUMN_PREFIX, endPrefixes); // MULTI change EdgeBFSReducer for multiple endPrefixes
//      opt.put("C.numEntriesCheckpoint", String.valueOf(numEntriesCheckpoint));

      if (Rtable != null && plusOp != null)
        GraphuloUtil.applyIteratorSoft(plusOp, tops, Rtable);
      if (RTtable != null && plusOp != null)
        GraphuloUtil.applyIteratorSoft(plusOp, tops, RTtable);
    }

    BatchScanner bs, bsDegree = null;
    try {
      bs = connector.createBatchScanner(Etable, Authorizations.EMPTY, 2); // TODO P2: set number of batch scan threads
      if (needDegreeFiltering)
        bsDegree = connector.createBatchScanner(ETDegtable, Authorizations.EMPTY, 4); // TODO P2: set number of batch scan threads
    } catch (TableNotFoundException e) {
      log.error("crazy", e);
      throw new RuntimeException(e);
    }

    String colFilterB = prependStartPrefix(endPrefixes, null); // MULTI
    log.debug("fetchColumn "+colFilterB);
//    bs.fetchColumn(EMPTY_TEXT, new Text(GraphuloUtil.prependStartPrefix(endPrefixes, v0, null)));

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
              ? filterTextsDegreeTable(bsDegree, degColumnText, degInColQ, minDegree, maxDegree, vktexts, GraphuloUtil.d4mRowToRanges(v0))
              : filterTextsDegreeTable(bsDegree, degColumnText, degInColQ, minDegree, maxDegree, vktexts);
          dur = System.currentTimeMillis() - t1;
          degTime += dur;
          if (trace) {
            System.out.println("Degree Lookup Time: " + dur + " ms");
            System.out.println("k=" + thisk + " after  filter" +
                (vktexts.size() > 5 ? " #=" + String.valueOf(vktexts.size()) : ": " + vktexts.toString()));
          }

          if (vktexts.isEmpty())
            break;
          opt.put("AT.colFilter", prependStartPrefix(startPrefixes, vktexts)); // MULTI

        } else {  // no filtering
          if (thisk == 1) {
            opt.put("AT.colFilter", GraphuloUtil.padD4mString(startPrefixes, ",", v0));
          } else
            opt.put("AT.colFilter", prependStartPrefix(startPrefixes, vktexts));
        }
        log.debug("AT.colFilter: " + opt.get("AT.colFilter"));

        bs.setRanges(Collections.singleton(new Range()));
        bs.clearScanIterators();
        bs.clearColumns();
//        GraphuloUtil.applyGeneralColumnFilter(colFilterB, bs, 4, false);
        opt.put("B.colFilter", colFilterB);
        IteratorSetting itset = new IteratorSetting(EScanIteratorPriority, TableMultIterator.class, opt);
        bs.addScanIterator(itset);

        EdgeBFSReducer reducer = new EdgeBFSReducer();
        reducer.init(Collections.singletonMap(EdgeBFSReducer.IN_COLUMN_PREFIX, endPrefixes), null);
        long t2 = System.currentTimeMillis();
        for (Map.Entry<Key, Value> entry : bs) {
//        System.out.println("A Entry: "+entry.getKey() + " -> " + entry.getValue());
          RemoteWriteIterator.decodeValue(entry.getValue(), reducer);
        }
        long dur = System.currentTimeMillis() - t2;
        scanTime += dur;

        vktexts.clear();
        for (String uk : reducer.getSerializableForClient()) {
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
      if (bsDegree != null)
        bsDegree.close();
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
    scan.close();
    return sb.toString();
  }


  /** Essentially does same thing as {@link GraphuloUtil#padD4mString}.
   * When vktexts is null, similar to {@link GraphuloUtil#singletonsAsPrefix}.
   */
  static String prependStartPrefix(String prefixes, Collection<Text> vktexts) {
    if (prefixes == null || prefixes.isEmpty() ||
        (GraphuloUtil.d4mStringContainsEmptyString(prefixes) && vktexts == null))
      return prependStartPrefix_Single("", GraphuloUtil.DEFAULT_SEP_D4M_STRING, vktexts);
    char sep = prefixes.charAt(prefixes.length()-1);
    String s = "";
    for (String prefix : GraphuloUtil.splitD4mString(prefixes)) {
      s += prependStartPrefix_Single(prefix, sep, vktexts);
    }
    return s;
  }

  /**
   * Todo? Replace with {@link Range#prefix} or {@link Range#followingPrefix(Text)}.
   * May break with unicode.
   * @param prefix e.g. "out|"
   * @param vktexts Set of nodes like "v1,v3,v0,"
   * @return "out|v1,out|v3,out|v0," or "out|,:,out}," if vktexts is null or empty
   */
  static String prependStartPrefix_Single(String prefix, char sep, Collection<Text> vktexts) {
    if (prefix == null)
      prefix = "";
    if (vktexts == null || vktexts.isEmpty()) {
      if (prefix.isEmpty())
        return ":"+sep;
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
   * @param computeInDegrees True means compute the in-degrees of nodes reached in Rtable that are not starting nodes for any BFS step.
   *                         Makes little sense to set this to true if copyOutDegrees is false. Invovles an extra scan at the client.
   * @param minDegree      Minimum out-degree. Checked before doing any searching, at every step, from SDegtable. Pass 0 for no filtering.
   * @param maxDegree      Maximum out-degree. Checked before doing any searching, at every step, from SDegtable. Pass Integer.MAX_VALUE for no filtering.
   * @param plusOp         An SKVI to apply to the result table that "sums" values. Not applied if null.
   *                       Be careful: this affects degrees in the result table as well as normal entries.
   * @param outputUnion    Whether to output nodes reachable in EXACTLY or UP TO k BFS steps.
   * @param trace          Enable server-side performance tracing.
   * @return  The nodes reachable in EXACTLY k steps from v0.
   *          If outputUnion is true, then returns the nodes reachable in UP TO k steps from v0.
   */
  @SuppressWarnings("unchecked")
  public String SingleBFS(String Stable, String edgeColumn, char edgeSep,
                          String v0, int k, String Rtable,
                          String SDegtable, String degColumn, boolean degInColQ,
                          boolean copyOutDegrees, boolean computeInDegrees, int minDegree, int maxDegree,
                          IteratorSetting plusOp, boolean outputUnion, boolean trace) {
    boolean needDegreeFiltering = minDegree > 1 || maxDegree < Integer.MAX_VALUE;
    checkGiven(true, "Stable", Stable);
    if (needDegreeFiltering && (SDegtable == null || SDegtable.isEmpty()))
      throw new IllegalArgumentException("Please specify SDegtable since filtering is required. Given: " + Stable);
    Rtable = emptyToNull(Rtable);
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
    Collection<Text> vktexts = new HashSet<Text>(); //v0 == null ? null : GraphuloUtil.d4mRowToTexts(v0);
    /** If no degree filtering and v0 is a range, then does not include v0 nodes. */
    Collection<Text> mostAllOutNodes = null;
    Collection<Text> allInNodes = null;
    if (computeInDegrees || outputUnion)
      allInNodes = new HashSet<Text>();
    if (computeInDegrees)
      mostAllOutNodes = new HashSet<Text>();
    char sep = v0.charAt(v0.length() - 1);

    TableOperations tops = connector.tableOperations();
    if (!tops.exists(Stable))
      throw new IllegalArgumentException("Table A does not exist. Given: " + Stable);
    if (Rtable != null && !tops.exists(Rtable))
      try {
        tops.create(Rtable);
      } catch (AccumuloException e) {
        log.error("error trying to create R table " + Rtable, e);
        throw new RuntimeException(e);
      } catch (AccumuloSecurityException e) {
        log.error("error trying to create R table " + Rtable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("crazy", e);
        throw new RuntimeException(e);
      }

    Map<String, String> optSingleReducer = new HashMap<String, String>(), optSTI = new HashMap<String, String>();
    optSTI.put(SingleTransposeIterator.EDGESEP, edgeSepStr);
    optSTI.put(SingleTransposeIterator.NEG_ONE_IN_DEG, Boolean.toString(false)); // not a good option
    optSTI.put(SingleTransposeIterator.DEGCOL, degColumn);
    optSingleReducer.put(SingleBFSReducer.EDGE_SEP, edgeSepStr);

    BatchScanner bs, bsDegree = null;
    try {
      bs = connector.createBatchScanner(Stable, Authorizations.EMPTY, 50); // TODO P2: set number of batch scan threads
      if (needDegreeFiltering)
        bsDegree = connector.createBatchScanner(SDegtable, Authorizations.EMPTY, 4); // TODO P2: set number of batch scan threads
    } catch (TableNotFoundException e) {
      log.error("crazy", e);
      throw new RuntimeException(e);
    }



    try {
      long degTime = 0, scanTime = 0;
      for (int thisk = 1; thisk <= k; thisk++) {
        if (trace)
          if (thisk == 1)
            System.out.println("First step: v0 is " + v0);
          else
            System.out.println("k=" + thisk + " before filter" +
                (vktexts.size() > 5 ? " #=" + String.valueOf(vktexts.size()) : ": " + vktexts.toString()));

        Collection<Range> rowFilter;
        if (needDegreeFiltering /*&& SDegtable != null*/) { // use degree table
          long t1 = System.currentTimeMillis(), dur;
          vktexts = thisk == 1
              ? filterTextsDegreeTable(bsDegree, degColumnText, degInColQ, minDegree, maxDegree, vktexts, GraphuloUtil.d4mRowToRanges(v0))
              : filterTextsDegreeTable(bsDegree, degColumnText, degInColQ, minDegree, maxDegree, vktexts);
          dur = System.currentTimeMillis() - t1;
          degTime += dur;
          if (trace) {
            System.out.println("Degree Lookup Time: " + dur + " ms");
            System.out.println("k=" + thisk + " after  filter" +
                (vktexts.size() > 5 ? " #=" + String.valueOf(vktexts.size()) : ": " + vktexts.toString()));
          }

          if (vktexts.isEmpty())
            break;
          if (mostAllOutNodes != null)
            mostAllOutNodes.addAll(vktexts);
//          opt.put("rowRanges", GraphuloUtil.singletonsAsPrefix(vktexts, sep));
          rowFilter = GraphuloUtil.d4mRowToRanges(GraphuloUtil.singletonsAsPrefix(vktexts, sep));
          optSTI.put(SingleTransposeIterator.STARTNODES, GraphuloUtil.singletonsAsPrefix(vktexts, sep));

        } else {  // no filtering
          if (thisk == 1) {
//            opt.put("rowRanges", GraphuloUtil.singletonsAsPrefix(v0));
            rowFilter = GraphuloUtil.d4mRowToRanges(GraphuloUtil.singletonsAsPrefix(v0));
            optSTI.put(SingleTransposeIterator.STARTNODES, v0);
          } else {
            if (mostAllOutNodes != null)
              mostAllOutNodes.addAll(vktexts);
//            opt.put("rowRanges", GraphuloUtil.singletonsAsPrefix(vktexts, sep));
            rowFilter = GraphuloUtil.d4mRowToRanges(GraphuloUtil.singletonsAsPrefix(vktexts, sep));
            optSTI.put(SingleTransposeIterator.STARTNODES, GraphuloUtil.textsToD4mString(vktexts, sep));
          }
        }

        bs.fetchColumn(EMPTY_TEXT, edgeColumnText);
        if (copyOutDegrees)
          bs.fetchColumn(EMPTY_TEXT, degColumnText);

        List<IteratorSetting> iteratorSettingList = Collections.singletonList(
          new IteratorSetting(1, SingleTransposeIterator.class, optSTI));

        SingleBFSReducer reducer = new SingleBFSReducer();
        reducer.init(optSingleReducer, null);

        long t2 = System.currentTimeMillis();
        OneTable(Stable, Rtable, null, null, // feature addition: could gather entries at the client
            4, reducer, optSingleReducer,
            plusOp, rowFilter, null, // column filter applied through BatchScanner fetchColumn
            iteratorSettingList, bs, trace);
        long dur = System.currentTimeMillis() - t2;
        scanTime += dur;

        vktexts.clear();
//      vktexts.addAll(reducer.getSerializableForClient());
        for (String uk : reducer.getSerializableForClient()) {
          vktexts.add(new Text(uk));
        }
        if (trace)
          System.out.println("BatchScan/Iterator Time: " + dur + " ms");
        if (vktexts.isEmpty())
          break;
        if (allInNodes != null)
          allInNodes.addAll(vktexts);
      }

      if (trace) {
        System.out.println("Total Degree Lookup Time: " + degTime + " ms");
        System.out.println("Total BatchScan/Iterator Time: " + scanTime + " ms");
      }

    } finally {
      bs.close();
      if (bsDegree != null)
        bsDegree.close();
    }

    // take union if necessary
    String ret = GraphuloUtil.textsToD4mString(outputUnion ? allInNodes : vktexts, sep);

    // compute in degrees if necessary
    if (computeInDegrees) {
      allInNodes.removeAll(mostAllOutNodes);
//      log.debug("allInNodes: "+allInNodes);
      singleCheckWriteDegrees(allInNodes, Stable, Rtable, degColumn.getBytes(), edgeSepStr, sep);
    }

    return ret;
  }

  private void singleCheckWriteDegrees(Collection<Text> questionNodes, String Stable, String Rtable,
                                       byte[] degColumn, String edgeSepStr, char sep) {
    Scanner scan;
    BatchWriter bw;
    try {
      scan = connector.createScanner(Rtable, Authorizations.EMPTY);
      bw = connector.createBatchWriter(Rtable, new BatchWriterConfig());
    } catch (TableNotFoundException e) {
      log.error("crazy", e);
      throw new RuntimeException(e);
    }

    //GraphuloUtil.singletonsAsPrefix(questionNodes, sep);
    // There is a more efficient way to do this:
    //  scan all the ranges at once with a BatchScanner, somehow preserving the order
    //  such that all the v1, v1|v4, v1|v6, ... appear in order.
    // This is likely not a bottleneck.
    List<Range> rangeList;
    {
      Collection<Range> rs = new TreeSet<Range>();
      for (Text node : questionNodes) {
        rs.add(Range.prefix(node));
      }
      rangeList = Range.mergeOverlapping(rs);
    }

    try {
      Text row = new Text();
      RANGELOOP: for (Range range : rangeList) {
//        log.debug("range: "+range);
        boolean first = true;
        int cnt = 0, pos = -1;
        scan.setRange(range);
        for (Map.Entry<Key, Value> entry : scan) {
//          log.debug(entry.getKey()+" -> "+entry.getValue());
          if (first) {
            pos = entry.getKey().getRow(row).find(edgeSepStr);
            if (pos == -1)
              continue RANGELOOP;  // this is a degree node; no need to re-write degree
            first = false;
          }
          // assume all other rows in this range are degree rows.
          cnt++;
        }
        // row contains "v1|v2" -- want v1. pos is the byte position of the '|'. cnt is the degree.
        Mutation m = new Mutation(row.getBytes(), 0, pos);
        m.put(GraphuloUtil.EMPTY_BYTES, degColumn, String.valueOf(cnt).getBytes());
        bw.addMutation(m);
      }


    } catch (MutationsRejectedException e) {
      log.error("canceling in-degree ingest because in-degree mutations rejected, sending to Rtable "+Rtable, e);
      throw new RuntimeException(e);
    } finally {
      scan.close();
      try {
        bw.close();
      } catch (MutationsRejectedException e) {
        log.error("in-degree mutations rejected, sending to Rtable "+Rtable, e);
      }
    }

  }


  /**
   * @param Atable   Accumulo adjacency table input
   * @param ATtable  Transpose of input adjacency table
   * @param Rtable   Result table, can be null. Created if nonexisting.
   * @param RTtable  Transpose of result table, can be null. Created if nonexisting.
   * @param BScanIteratorPriority Priority to use for Table Multiplication scan-time iterator on table B
   * @param isDirected True for directed input, false for undirected input
   * @param includeExtraCycles (only applies to directed graphs)
*                           True to include cycles between edges that come into the same node
   * @param plusOp    An SKVI to apply to the result table that "sums" values. Not applied if null.
*                  This has no effect if the result table is empty before the operation.
   * @param rowFilter Experimental. Pass null for no filtering.
   * @param colFilterAT Experimental. Pass null for no filtering.
   * @param colFilterB Experimental. Pass null for no filtering.
   * @param numEntriesCheckpoint # of entries before we emit a checkpoint entry from the scan. -1 means no monitoring.
   * @param trace  Enable server-side performance tracing.
   * @param separator The string to insert between the labels of two nodes.
*                  This string should not appear in any node label. Used to find the original nodes.
   */
  public void LineGraph(String Atable, String ATtable, String Rtable, String RTtable,
                        int BScanIteratorPriority, boolean isDirected, boolean includeExtraCycles, IteratorSetting plusOp,
                        Collection<Range> rowFilter, String colFilterAT, String colFilterB,
                        int numEntriesCheckpoint, boolean trace, String separator) {
    Map<String,String> opt = new HashMap<String, String>();
    opt.put("rowMultiplyOp", LineRowMultiply.class.getName());
    opt.put("rowMultiplyOp.opt."+LineRowMultiply.SEPARATOR, separator);
    opt.put("rowMultiplyOp.opt."+LineRowMultiply.ISDIRECTED, Boolean.toString(isDirected));
    opt.put("rowMultiplyOp.opt."+LineRowMultiply.INCLUDE_EXTRA_CYCLES, Boolean.toString(includeExtraCycles));

    TwoTable(ATtable, Atable, Rtable, RTtable, BScanIteratorPriority,
        TwoTableIterator.DOTMODE.ROW, opt, plusOp,
        rowFilter, colFilterAT, colFilterB,
        false, false, Collections.<IteratorSetting>emptyList(),
        Collections.<IteratorSetting>emptyList(), Collections.<IteratorSetting>emptyList(),
        null, null,
        numEntriesCheckpoint, trace);
  }

  protected String emptyToNull(String s) { return s != null && s.isEmpty() ? null : s; }

  /**
   * Count number of entries in a table using a BatchScanner with {@link CountAllIterator}.
   */
  public long countEntries(String table) {
    Preconditions.checkArgument(table != null && !table.isEmpty());

    BatchScanner bs;
    try {
      bs = connector.createBatchScanner(table, Authorizations.EMPTY, 2); // todo: 2 threads is arbitrary
    } catch (TableNotFoundException e) {
      log.error("table "+table+" does not exist", e);
      throw new RuntimeException(e);
    }

    bs.setRanges(Collections.singleton(new Range()));
    bs.addScanIterator(new IteratorSetting(30, CountAllIterator.class));

    long cnt = 0l;
    try {
      for (Map.Entry<Key, Value> entry : bs) {
        cnt += new Long(new String(entry.getValue().get()));
      }
    } finally {
      bs.close();
    }
    return cnt;
  }


  /**
   * From input <b>unweighted, undirected</b> adjacency table Aorig, put the k-Truss
   * of Aorig in Rfinal.
   * @param Aorig Unweighted, undirected adjacency table.
   * @param Rfinal Does not have to previously exist. Writes the kTruss into Rfinal if it already exists.
   *               Use a combiner if you want to sum it in.
   * @param k Trivial if k <= 2.
   * @param filterRowCol Filter applied to rows and columns of Aorig
   *                     (must apply to both rows and cols because A is undirected Adjacency table).
   * @param forceDelete False means throws exception if the temporary tables used inside the algorithm already exist.
   *                    True means delete them if they exist.
   * @param trace Server-side tracing.
   * @return nnz of the kTruss subgraph, which is 2* the number of edges in the kTruss subgraph.
   *          Returns -1 if k < 2 since there is no point in counting the number of edges.
   */
  public long kTrussAdj(String Aorig, String Rfinal, int k,
                        String filterRowCol, boolean forceDelete, boolean trace) {
    checkGiven(true, "Aorig", Aorig);
    Preconditions.checkArgument(Rfinal != null && !Rfinal.isEmpty(), "Output table must be given or operation is useless: Rfinal=%s", Rfinal);
    TableOperations tops = connector.tableOperations();
    boolean RfinalExists = tops.exists(Rfinal);

    try {
      if (k <= 2) {               // trivial case: every graph is a 2-truss
        if (RfinalExists || filterRowCol != null)
          OneTable(Aorig, Rfinal, null, null, -1, null, null, PLUS_ITERATOR_LONG,
                  filterRowCol == null ? null : GraphuloUtil.d4mRowToRanges(filterRowCol),
                  filterRowCol, null, null, trace);
        else
          tops.clone(Aorig, Rfinal, true, null, null);    // flushes Aorig before cloning
        return -1;
      }

      // non-trivial case: k is 3 or more.
      String Atmp, A2tmp, AtmpAlt;
      long nnzBefore, nnzAfter;
      String tmpBaseName = Aorig+"_kTrussAdj_";
      Atmp = tmpBaseName+"tmpA";
      A2tmp = tmpBaseName+"tmpA2";
      AtmpAlt = tmpBaseName+"tmpAalt";
      deleteTables(forceDelete, Atmp, A2tmp, AtmpAlt);

      if (filterRowCol == null) {
        tops.clone(Aorig, Atmp, true, null, null);
        nnzAfter = countEntries(Aorig);
      }
      else
        nnzAfter = OneTable(Aorig, Atmp, null, null, -1, null, null, null,
                filterRowCol == null ? null : GraphuloUtil.d4mRowToRanges(filterRowCol),
                filterRowCol, null, null, trace);

      // Inital nnz
      // Careful: nnz figure will be inaccurate if there are multiple versions of an entry in Aorig.
      // The truly accurate count is to count them first!
//      D4mDbTableOperations d4mtops = new D4mDbTableOperations(connector.getInstance().getInstanceName(),
//          connector.getInstance().getZooKeepers(), connector.whoami(), new String(password.getPassword()));
//      nnzAfter = d4mtops.getNumberOfEntries(Collections.singletonList(Aorig))
      // Above method dangerous. Instead:
      

      // Filter out entries with < k-2
      IteratorSetting sumAndFilter = new DynamicIteratorSetting()
        .append(PLUS_ITERATOR_LONG)
        .append(MinMaxValueFilter.iteratorSetting(10, ScalarType.LONG, k-2, null))
        .toIteratorSetting(PLUS_ITERATOR_LONG.getPriority());
      // No Diagonal filter
      List<IteratorSetting> noDiagFilter = Collections.singletonList(
          TriangularFilter.iteratorSetting(1, TriangularFilter.TriangularType.NoDiagonal));

      do {
        nnzBefore = nnzAfter;

        // Use Atmp for both AT and B
        TableMult(TwoTableIterator.CLONESOURCE_TABLENAME, Atmp, A2tmp, null, -1, ConstantTwoScalar.class, null,
            sumAndFilter, null, null, null, false, false,
            null, null, noDiagFilter,
            null, null, -1, trace);
        // A2tmp has a SummingCombiner

        nnzAfter = SpEWiseX(A2tmp, Atmp, AtmpAlt, null, -1, ConstantTwoScalar.class, null, null,
            null, null, null, -1, trace);

        tops.delete(Atmp);
        tops.delete(A2tmp);
        { String t = Atmp; Atmp = AtmpAlt; AtmpAlt = t; }

        log.debug("nnzBefore "+nnzBefore+" nnzAfter "+nnzAfter);
      } while (nnzBefore != nnzAfter);
      // Atmp, ATtmp have the result table. Could be empty.

      if (RfinalExists)  // sum whole graph into existing graph
        AdjBFS(Atmp, null, 1, Rfinal, null, null, -1, null, null, false, 0, Integer.MAX_VALUE, null, trace);
      else                                           // result is new;
        tops.clone(Atmp, Rfinal, true, null, null);  // flushes Atmp before cloning

      return nnzAfter;

    } catch (AccumuloException e) {
      log.error("Exception in kTrussAdj", e);
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      log.error("Exception in kTrussAdj", e);
      throw new RuntimeException(e);
    } catch (TableExistsException e) {
      log.error("Exception in kTrussAdj", e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("Exception in kTrussAdj", e);
      throw new RuntimeException(e);
    }
  }


  /**
   * From input <b>unweighted, undirected</b> incidence table Eorig, put the k-Truss
   * of Eorig in Rfinal.  Needs transpose ETorig, and can output transpose of k-Truss subgraph too.
   * @param Eorig Unweighted, undirected incidence table.
   * @param ETorig Transpose of input incidence table.
   *               Optional.  Created on the fly if not given.
   * @param Rfinal Does not have to previously exist. Writes the kTruss into Rfinal if it already exists.
   *               Use a combiner if you want to sum it in.
   * @param RTfinal Does not have to previously exist. Writes in the transpose of the kTruss subgraph.
   * @param k Trivial if k <= 2.
   * @param edgeFilter Filter on rows of Eorig, i.e., the edges in an incidence table.
   * @param forceDelete False means throws exception if the temporary tables used inside the algorithm already exist.
   *                    True means delete them if they exist.
   * @param trace Server-side tracing.
   * @return  nnz of the kTruss subgraph, which is 2* the number of edges in the kTruss subgraph.
   *          Returns -1 if k < 2 since there is no point in counting the number of edges.
   */
  public long kTrussEdge(String Eorig, String ETorig, String Rfinal, String RTfinal, int k,
                         Collection<Range> edgeFilter, boolean forceDelete, boolean trace) { // iterator priority?
    // small optimization possible: pass in Aorig = ET*E if present. Saves first iteration matrix multiply. Not really worth it.
    checkGiven(true, "Eorig", Eorig);
    Rfinal = emptyToNull(Rfinal);
    RTfinal = emptyToNull(RTfinal);
    ETorig = emptyToNull(ETorig);
    Preconditions.checkArgument(Rfinal != null || RTfinal != null, "One Output table must be given or operation is useless: Rfinal=%s; RTfinal=%s", Rfinal, RTfinal);
    TableOperations tops = connector.tableOperations();
    boolean RfinalExists = Rfinal != null && tops.exists(Rfinal),
        RTfinalExists = RTfinal != null && tops.exists(RTfinal);

    try {
      if (k <= 2) {               // trivial case: every graph is a 2-truss
        if (RfinalExists && RTfinalExists) { // sum whole graph into existing graph
          // AdjBFS works just as well as EdgeBFS because we're not doing any filtering.
          AdjBFS(Eorig, null, 1, Rfinal, RTfinal, null, -1, null, null, false, 0, Integer.MAX_VALUE, null, trace);
        } else if (RfinalExists) {
          AdjBFS(Eorig, null, 1, Rfinal, null, null, -1, null, null, false, 0, Integer.MAX_VALUE, null, trace);
          if (RTfinal != null)
            tops.clone(ETorig, RTfinal, true, null, null);
        } else if (RTfinalExists) {
          AdjBFS(Eorig, null, 1, null, RTfinal, null, -1, null, null, false, 0, Integer.MAX_VALUE, null, trace);
          if (Rfinal != null)
            tops.clone(Eorig, Rfinal, true, null, null);
        } else {                                          // both graphs are new;
          if (Rfinal != null)
            tops.clone(Eorig, Rfinal, true, null, null);  // flushes Eorig before cloning
          if (RTfinal != null)
            tops.clone(ETorig, RTfinal, true, null, null);
        }
        return -1;
      }

      // non-trivial case: k is 3 or more.
      String Etmp, ETtmp, Rtmp, Atmp, EtmpAlt, ETtmpAlt;
      long nnzBefore, nnzAfter;
      String tmpBaseName = Eorig+"_kTrussEdge_";
      Etmp = tmpBaseName+"tmpE";
      ETtmp = tmpBaseName+"tmpET";
      Atmp = tmpBaseName+"tmpA";
      Rtmp = tmpBaseName+"tmpR";
      EtmpAlt = tmpBaseName+"tmpEalt";
      ETtmpAlt = tmpBaseName+"tmpETalt";
      deleteTables(forceDelete, Etmp, ETtmp, Atmp, Rtmp, EtmpAlt, ETtmpAlt);

      if (edgeFilter == null && ETorig != null && tops.exists(ETorig)) {
        tops.clone(Eorig, Etmp, true, null, null);
        tops.clone(ETorig, ETtmp, true, null, null);
        nnzAfter = countEntries(Eorig);
      } else
        nnzAfter = OneTable(Eorig, Etmp, ETtmp, null, -1, null, null, null, edgeFilter, null, null, null, trace);

      // Inital nnz
      // Careful: nnz figure will be inaccurate if there are multiple versions of an entry in Aorig.
      // The truly accurate count is to count them first!
//      D4mDbTableOperations d4mtops = new D4mDbTableOperations(connector.getInstance().getInstanceName(),
//          connector.getInstance().getZooKeepers(), connector.whoami(), new String(password.getPassword()));
//      nnzAfter = d4mtops.getNumberOfEntries(Collections.singletonList(Eorig))
      // Above method dangerous. Instead:


      // No Diagonal Filter
      IteratorSetting noDiagFilter = TriangularFilter.iteratorSetting(1, TriangularFilter.TriangularType.NoDiagonal);
      // E*A -> sum -> ==2 -> Abs0 -> OnlyRow -> sum -> kTrussFilter -> TT_RowSelector
      IteratorSetting itsBeforeR = new DynamicIteratorSetting()
          .append(PLUS_ITERATOR_LONG)
          .append(MinMaxValueFilter.iteratorSetting(1, ScalarType.LONG, 2, 2))
          .append(ConstantTwoScalar.iteratorSetting(1, new Value("1".getBytes())))
          .append(KeyRetainOnlyApply.iteratorSetting(1, PartialKey.ROW))
          .append(PLUS_ITERATOR_LONG)
          .append(MinMaxValueFilter.iteratorSetting(10, ScalarType.LONG, k-2, null))
          .toIteratorSetting(PLUS_ITERATOR_LONG.getPriority());

      do {
        nnzBefore = nnzAfter;

        TableMult(TwoTableIterator.CLONESOURCE_TABLENAME, Etmp, Atmp, null, -1, ConstantTwoScalar.class, null,
            PLUS_ITERATOR_LONG, null, null, null, false, false, null, null,
            Collections.singletonList(noDiagFilter), null, null, -1, trace);
        // Atmp has a SummingCombiner

        TableMult(ETtmp, Atmp, Rtmp, null, ConstantTwoScalar.class, itsBeforeR);
        // Rtmp has a big DynamicIterator
        tops.delete(ETtmp);
        tops.delete(Atmp);

        // E*A -> sum -> ==2 -> Abs0 -> OnlyRow -> sum -> kTrussFilter -> TT_RowSelector <- E
        //                                                                \-> Writing to EtmpAlt, ETtmpAlt
        nnzAfter = TwoTableROWSelector(Rtmp, Etmp, EtmpAlt, ETtmpAlt, -1, null, null, null, true,
            null, null, null, null, null, -1, trace);
        tops.delete(Etmp);
        tops.delete(Rtmp);

        { String t = Etmp; Etmp = EtmpAlt; EtmpAlt = t; }
        { String t = ETtmp; ETtmp = ETtmpAlt; ETtmpAlt = t; }

        log.debug("nnzBefore "+nnzBefore+" nnzAfter "+nnzAfter);
      } while (nnzBefore != nnzAfter);
      // Etmp, ETtmp have the result table. Could be empty.

      if (RfinalExists && RTfinalExists) { // sum whole graph into existing graph
        // AdjBFS works just as well as EdgeBFS because we're not doing any filtering.
        AdjBFS(Etmp, null, 1, Rfinal, RTfinal, null, -1, null, null, false, 0, Integer.MAX_VALUE, null, trace);
      } else if (RfinalExists) {
        AdjBFS(Etmp, null, 1, Rfinal, null, null, -1, null, null, false, 0, Integer.MAX_VALUE, null, trace);
        if (RTfinal != null)
          tops.clone(ETtmp, RTfinal, true, null, null);
      } else if (RTfinalExists) {
        AdjBFS(Etmp, null, 1, null, RTfinal, null, -1, null, null, false, 0, Integer.MAX_VALUE, null, trace);
        if (Rfinal != null)
          tops.clone(Etmp, Rfinal, true, null, null);
      } else {                                          // both graphs are new;
        if (Rfinal != null)
          tops.clone(Etmp, Rfinal, true, null, null);  // flushes Etmp before cloning
        if (RTfinal != null)
          tops.clone(ETtmp, RTfinal, true, null, null);
      }

      return nnzAfter;

    } catch (AccumuloException e) {
      log.error("Exception in kTrussAdj", e);
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      log.error("Exception in kTrussAdj", e);
      throw new RuntimeException(e);
    } catch (TableExistsException e) {
      log.error("Exception in kTrussAdj", e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("Exception in kTrussAdj", e);
      throw new RuntimeException(e);
    }
  }



  /**
   * From input <b>unweighted, undirected</b> adjacency table Aorig,
   * put the Jaccard coefficients in the upper triangle of Rfinal.
   * @param Aorig Unweighted, undirected adjacency table.
   * @param Rfinal Should not previously exist. Writes the Jaccard table into Rfinal,
   *               using a couple combiner-like iterators.
   * @param filterRowCol Filter applied to rows and columns of Aorig
   *                     (must apply to both rows and cols because A is undirected Adjacency table).
   * @param trace Server-side tracing.
   * @return number of partial products sent to Rtable during the Jaccard coefficient calculation
   */
  public long Jaccard(String Aorig, String ADeg, String Rfinal,
                      String filterRowCol, boolean trace) {
    checkGiven(true, "Aorig, ADeg", Aorig, ADeg);
    Preconditions.checkArgument(Rfinal != null && !Rfinal.isEmpty(), "Output table must be given or operation is useless: Rfinal=%s", Rfinal);
    TableOperations tops = connector.tableOperations();
    Preconditions.checkArgument(!tops.exists(Rfinal), "Output Jaccard table must not exist: Rfinal=%s", Rfinal); // this could be relaxed, at the possibility of peril

    // "Plus" iterator to set on Rfinal
    IteratorSetting RPlusIteratorSetting = new DynamicIteratorSetting()
      .append(MathTwoScalar.combinerSetting(1, null, ScalarOp.PLUS, ScalarType.LONG))
      .append(JaccardDegreeApply.iteratorSetting(1, basicRemoteOpts(ApplyIterator.APPLYOP + ApplyIterator.OPT_SUFFIX, ADeg)))
      .toIteratorSetting(PLUS_ITERATOR_BIGDECIMAL.getPriority());

    // use a deepCopy of the local iterator on A for the left part of the TwoTable
    long npp = TableMult(TwoTableIterator.CLONESOURCE_TABLENAME, Aorig, Rfinal, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(ScalarOp.TIMES, ScalarType.LONG),    // this could be a ConstantTwoScalar if we knew no "0" entries present
        RPlusIteratorSetting,
        filterRowCol == null ? null : GraphuloUtil.d4mRowToRanges(filterRowCol),
        filterRowCol, filterRowCol,
        true, true,
        Collections.singletonList(TriangularFilter.iteratorSetting(1, TriangularFilter.TriangularType.Lower)),
        Collections.singletonList(TriangularFilter.iteratorSetting(1, TriangularFilter.TriangularType.Upper)),
        Collections.singletonList(TriangularFilter.iteratorSetting(1, TriangularFilter.TriangularType.Upper)),
        null, null, -1, trace);

    log.debug("Jaccard #partial products " + npp);
    return npp;
  }


  /**
   * Create a degree table from an existing table.
   * @param table Name of original table.
   * @param Degtable Name of degree table. Created if it does not exist.
   *                 Use a combiner if you want to sum in the new degree entries into an existing table.
   * @param countColumns True means degrees are the <b>number of entries in each row</b>.
   *                     False means degrees are the <b>sum or weights of entries in each row</b>.
   * @param trace Server-side tracing.
   * @return The number of rows in the original table.
   */
  public long generateDegreeTable(String table, String Degtable, boolean countColumns, boolean trace) {
    checkGiven(true, "table", table);
    Preconditions.checkArgument(Degtable != null && !Degtable.isEmpty(), "Output table must be given: Degtable=%s", Degtable);
    TableOperations tops = connector.tableOperations();
    BatchScanner bs;
    try {
      if (!tops.exists(Degtable))
        tops.create(Degtable);
      bs = connector.createBatchScanner(table, Authorizations.EMPTY, 2); // todo: 2 threads arbitrary
    } catch (TableNotFoundException e) {
      log.error("crazy", e);
      throw new RuntimeException(e);
    } catch (TableExistsException e) {
      log.error("crazy", e);
      throw new RuntimeException(e);
    } catch (AccumuloException e) {
      log.error("problem creating degree table "+Degtable, e);
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      log.error("problem creating degree table "+Degtable, e);
      throw new RuntimeException(e);
    }
    bs.setRanges(Collections.singleton(new Range()));

    {
      DynamicIteratorSetting dis = new DynamicIteratorSetting();
      if (countColumns)
          dis.append(ConstantTwoScalar.iteratorSetting(1, new Value("1".getBytes()))); // Abs0
      dis
        .append(KeyRetainOnlyApply.iteratorSetting(1, PartialKey.ROW))
        .append(PLUS_ITERATOR_BIGDECIMAL)
        .append(new IteratorSetting(1, RemoteWriteIterator.class, basicRemoteOpts("", Degtable)));
      bs.addScanIterator(dis.toIteratorSetting(PLUS_ITERATOR_BIGDECIMAL.getPriority()));
    }

    long totalRows = 0;
    try {
      for (Map.Entry<Key, Value> entry : bs) {
        totalRows += RemoteWriteIterator.decodeValue(entry.getValue(), null);
      }
    } finally {
      bs.close();
    }

    return totalRows;
  }

  private Map<String,String> basicRemoteOpts(String prefix, String remoteTable) {
    return basicRemoteOpts(prefix, remoteTable, null);
  }

  /**
   * Create the basic iterator settings for the {@link RemoteWriteIterator}.
   * @param prefix A prefix to apply to keys in the option map, e.g., the "B" in "B.tableName".
   * @param remoteTable Name of table to write to. Null does not put in the table name.
   * @param remoteTableTranspose Name of table to write transpose to. Null does not put in the transpose table name.
   * @return The basic set of options for {@link RemoteWriteIterator}.
   */
  private Map<String,String> basicRemoteOpts(String prefix, String remoteTable, String remoteTableTranspose) {
    if (prefix == null) prefix = "";
    Map<String,String> opt = new HashMap<String, String>();
    String instance = connector.getInstance().getInstanceName();
    String zookeepers = connector.getInstance().getZooKeepers();
    String user = connector.whoami();
    opt.put(prefix+"zookeeperHost", zookeepers);
    opt.put(prefix + "instanceName", instance);
    if (remoteTable != null)
      opt.put(prefix+"tableName", remoteTable);
    if (remoteTableTranspose != null)
      opt.put(prefix+"tableNameTranspose", remoteTableTranspose);
    opt.put(prefix + "username", user);
    opt.put(prefix+"password", new String(password.getPassword()));
    return opt;
  }


  /** Count the number of unique rows in an existing table. */
  public long countRows(String table) {
    Preconditions.checkArgument(table != null && !table.isEmpty());

    BatchScanner bs;
    try {
      bs = connector.createBatchScanner(table, Authorizations.EMPTY, 2); // todo: 2 threads is arbitrary
    } catch (TableNotFoundException e) {
      log.error("table "+table+" does not exist", e);
      throw new RuntimeException(e);
    }
    bs.setRanges(Collections.singleton(new Range()));

    bs.addScanIterator(new DynamicIteratorSetting()
        .append(KeyRetainOnlyApply.iteratorSetting(1, PartialKey.ROW))  // strip to row field
        .append(new IteratorSetting(1, VersioningIterator.class))       // only count a row once
        .append(ConstantTwoScalar.iteratorSetting(1, new Value("1".getBytes()))) // Abs0
        .append(KeyRetainOnlyApply.iteratorSetting(1, null))            // strip all fields
        .append(PLUS_ITERATOR_BIGDECIMAL)                                  // Sum
        .toIteratorSetting(10));

    long cnt = 0l;
    try {
      for (Map.Entry<Key, Value> entry : bs) {
        cnt += new Long(new String(entry.getValue().get()));
      }
    } finally {
      bs.close();
    }
    return cnt;
  }

  /** Delete tables. If they already exist, delete and re-create them if forceDelete==true,
   * otherwise throw an IllegalStateException. */
  private void deleteTables(boolean forceDelete, String... tns) {
    GraphuloUtil.deleteTables(connector, forceDelete, tns);
  }

  /** Ensure arugments are not null and not empty. If mustExist, ensures the arguments exist as Accumulo tables. */
  private void checkGiven(boolean mustExist, String varnames, String... args) {
    TableOperations tops = connector.tableOperations();
    String[] varnamesArr = varnames.split(",");
    assert varnamesArr.length == args.length;
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      Preconditions.checkArgument(arg != null && !arg.isEmpty(),
          "%s: must be given", varnamesArr[i].trim());
      if (mustExist)
        Preconditions.checkArgument(tops.exists(arg), "%s: %s does not exist", varnamesArr[i].trim(), arg);
    }
  }

  /**
   * Non-negative matrix factorization.
   * The main NMF loop stops when either (1) we reach the maximum number of iterations or
   *    (2) when the absolute difference in error between A and W*H is less than 0.01 from one iteration to the next.
   *
   *
   * @param Aorig Accumulo input table to factor
   * @param ATorig Transpose of Accumulo input table to factor
   * @param Wfinal Output table W. Must not exist before this method.
   * @param WTfinal Transpose of output table W. Must not exist before this method.
   * @param Hfinal Output table H. Must not exist before this method.
   * @param HTfinal Transpose of output table H. Must not exist before this method.
   * @param K Number of topics.
   * @param maxiter Maximum number of iterations
   * @param forceDelete Forcibly delete temporary tables used if they happen to exist. If false, throws an exception if they exist.
   * @param trace Enable server-side tracing.
   * @return The absolute difference in error (between A and W*H) from the last iteration to the second-to-last.
   */
  public double NMF(String Aorig, String ATorig,
                    String Wfinal, String WTfinal, String Hfinal, String HTfinal,
                    final int K, final int maxiter,
                    boolean forceDelete, boolean trace) {
    checkGiven(true, "Aorig, ATorig", Aorig, ATorig);
    checkGiven(false, "Wfinal, WTfinal, Hfinal, HTfinal", Wfinal, WTfinal, Hfinal, HTfinal);
    Preconditions.checkArgument(K > 0, "# of topics K must be > 0: "+K);
    deleteTables(false, Wfinal, WTfinal, Hfinal, HTfinal);

    String Ttmp1, Ttmp2;
    String tmpBaseName = Aorig+"_NMF_";
    Ttmp1 = tmpBaseName+"tmp1";
    Ttmp2 = tmpBaseName+"tmp2";
    deleteTables(forceDelete, Ttmp1, Ttmp2);

    // Initialize W to a dense random matrix of size N x K
    List<IteratorSetting> itCreateTopicList = new DynamicIteratorSetting()
        .append(KeyRetainOnlyApply.iteratorSetting(1, PartialKey.ROW))  // strip to row field
        .append(new IteratorSetting(1, VersioningIterator.class))       // only count a row once
        .append(RandomTopicApply.iteratorSetting(1, K))
        .getIteratorSettingList();

    // can replace with try-with-resources in Java 1.7\
    TraceScope scope = null;
    try {
      scope = Trace.startSpan("nmfCreateRandW", Sampler.ALWAYS);
      long NK = OneTable(Aorig, Wfinal, WTfinal, null, -1, null, null, null, null, null, itCreateTopicList, null, trace);
    } finally {
      if (scope != null)
        scope.close();
    }
    if (trace)
      DebugUtil.printTable("0: W is NxK:", connector, Wfinal);

    // No need to actually measure N and M
////    long N = countRows(Aorig);
//    assert NK % K == 0;
//    long N = NK / K;
//    long M = countRows(ATorig);

    // newerr starts at frobenius norm of A, since H starts at the zero matrix.
    double newerr = 0, olderr;
    int numiter = 0;

    do {
      numiter++;
      olderr = newerr;

      scope = null;
try {
  scope = Trace.startSpan("nmfStepToH", Sampler.ALWAYS);
        nmfStep(K, Wfinal, Aorig, Hfinal, HTfinal, Ttmp1, Ttmp2, trace);
      } finally {
  if (scope != null)
    scope.close();
}
      if (trace)
        DebugUtil.printTable(numiter + ": H is KxM:", connector, Hfinal);
      scope = null;
try {
  scope = Trace.startSpan("nmfStepToW", Sampler.ALWAYS);
        nmfStep(K, HTfinal, ATorig, WTfinal, Wfinal, Ttmp1, Ttmp2, trace);
      } finally {
  if (scope != null)
    scope.close();
}
      if (trace)
        DebugUtil.printTable(numiter + ": W is NxK:", connector, Wfinal);

      scope = null;
try {
  scope = Trace.startSpan("nmfFro", Sampler.ALWAYS);
        newerr = nmfDiffFrobeniusNorm(Aorig, WTfinal, Hfinal, Ttmp1, trace);
      } finally {
  if (scope != null)
    scope.close();
}
      if (trace)
        DebugUtil.printTable(numiter + ": A is NxM --- error is "+newerr+":", connector, Aorig);

      log.debug("NMF Iteration "+numiter+": olderr " + olderr + " newerr " + newerr);
    } while (Math.abs(newerr - olderr) > 0.01d && numiter < maxiter);

    return Math.abs(newerr - olderr);
  }


  private double nmfDiffFrobeniusNorm(String Aorig, String WTfinal, String Hfinal, String WHtmp, boolean trace) {
    // assume WHtmp has no entries / does not exist

    // Step 1: W*H => WHtmp
    TableMult(WTfinal, Hfinal, WHtmp, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(ScalarOp.TIMES, ScalarType.DOUBLE),
        MathTwoScalar.combinerSetting(PLUS_ITERATOR_BIGDECIMAL.getPriority(), null, ScalarOp.PLUS, ScalarType.DOUBLE),
        null, null, null, false, false, -1, false);
    if (trace)
      DebugUtil.printTable("WH is NxM:", connector, WHtmp);

    // Step 2: A - WH => ^2 => ((+all)) => Client w/ Reducer => Sq.Root. => newerr return
    // Prep.
    List<IteratorSetting> iterAfterMinus = new DynamicIteratorSetting()
        .append(MathTwoScalar.applyOpDouble(1, true, ScalarOp.POWER, 2.0))
//        .append(MathTwoScalar.combinerSetting(1, null, ScalarOp.PLUS, ScalarType.DOUBLE))
        .getIteratorSettingList();
    Map<String,String> sumReducerOpts = MathTwoScalar.optionMap(ScalarOp.PLUS, ScalarType.DOUBLE);
    MathTwoScalar sumReducer = new MathTwoScalar();
    sumReducer.init(sumReducerOpts, null);

    // Execute. Sum into sumReducer.
    SpEWiseSum(Aorig, WHtmp, null, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(ScalarOp.MINUS, ScalarType.DOUBLE),
        null, null, null, null, null, null,
        iterAfterMinus,
        sumReducer, sumReducerOpts,
        -1, false);

    // Delete temporary WH table.
    deleteTables(true, WHtmp);

    if (!sumReducer.hasTopForClient())
      return 0.0; // no error. This will never happen realistically.
    return Math.sqrt(Double.parseDouble(new String(sumReducer.getForClient())));
  }

  private void nmfStep(int K, String in1, String in2, String out1, String out2, String tmp1, String tmp2, boolean trace) {
    // delete out1, out2
    deleteTables(true, out1, out2);
    org.junit.Assert.assertTrue(Trace.isTracing());

    // Step 1: in1^T * in1 ==transpose==> tmp1
    TraceScope scope = null;
try {
  scope = Trace.startSpan("nmf1TableMult");
      TableMult(in1, in1, null, tmp1, -1,
          MathTwoScalar.class, MathTwoScalar.optionMap(ScalarOp.TIMES, ScalarType.DOUBLE),
          MathTwoScalar.combinerSetting(PLUS_ITERATOR_BIGDECIMAL.getPriority(), null, ScalarOp.PLUS, ScalarType.DOUBLE),
          null, null, null, false, false, -1, false);
    } finally {
  if (scope != null)
    scope.close();
}
    if (trace)
      DebugUtil.printTable("tmp1 is KxK:", connector, tmp1);

    // Step 2: tmp1 => tmp1 inverse.
    scope = null;
try {
  scope = Trace.startSpan("nmf2Inverse");
      connector.tableOperations().compact(tmp1, null, null,
          Collections.singletonList(InverseMatrixIterator.iteratorSetting(PLUS_ITERATOR_BIGDECIMAL.getPriority() + 1, K)),
          true, true); // blocks
    } catch (AccumuloException e) {
      log.error("problem while compacting "+tmp1+" to take the matrix inverse", e);
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      log.error("problem while compacting "+tmp1+" to take the matrix inverse", e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("crazy", e);
      throw new RuntimeException(e);
    } finally {
      if (scope != null)
        scope.close();
    }
    if (trace)
      DebugUtil.printTable("tmp1 INVERSE is KxK:", connector, tmp1);

    // Step 3: in1^T * in2 => tmp2.  This can run concurrently with step 1 and 2.
    scope = null;
try {
  scope = Trace.startSpan("nmf3TableMult");
      TableMult(in1, in2, tmp2, null, -1,
          MathTwoScalar.class, MathTwoScalar.optionMap(ScalarOp.TIMES, ScalarType.DOUBLE),
          MathTwoScalar.combinerSetting(PLUS_ITERATOR_BIGDECIMAL.getPriority(), null, ScalarOp.PLUS, ScalarType.DOUBLE),
          null, null, null, false, false, -1, false);
    } finally {
  if (scope != null)
    scope.close();
}

    // Step 4: tmp1^T * tmp2 => OnlyPositiveFilter => {out1, tranpsose to out2}
    // Filter out entries <= 0 after combining partial products.
    IteratorSetting sumFilterOp =
        new DynamicIteratorSetting()
        .append(MathTwoScalar.combinerSetting(1, null, ScalarOp.PLUS, ScalarType.DOUBLE))
        .append(MinMaxValueFilter.iteratorSetting(1, ScalarType.DOUBLE, Double.MIN_NORMAL, Double.MAX_VALUE))
        .toIteratorSetting(PLUS_ITERATOR_BIGDECIMAL.getPriority());

    // Execute.
    scope = null;
try {
  scope = Trace.startSpan("nmf4TableMult");
      TableMult(tmp1, tmp2, out1, out2, -1,
          MathTwoScalar.class, MathTwoScalar.optionMap(ScalarOp.TIMES, ScalarType.DOUBLE),
          sumFilterOp,
          null, null, null, false, false, null, null,
          null,
          null, null, -1, false);
    } finally {
  if (scope != null)
    scope.close();
}

    // Delete temporary tables.
    deleteTables(true, tmp1, tmp2);
  }



  /**
   * Non-negative matrix factorization <b>at the client</b>.
   * The main NMF loop stops when either (1) we reach the maximum number of iterations or
   *    (2) when the absolute difference in error between A and W*H is less than 0.01 from one iteration to the next.
   *
   *
   * @param Aorig Accumulo input table to factor
   * @param transposeA Whether to take the transpose of A (false if not).
   * @param Wfinal Output table W. Must not exist before this method. Can be null. Wfinal or Hfinal must be given.
   * @param transposeW Whether to write the transpose of W instead of W (false if not).
   * @param Hfinal Output table H. Must not exist before this method. Can be null. Wfinal or Hfinal must be given.
   * @param transposeH Whether to write the transpose of H instead of H (false if not).
   * @param K Number of topics.
   * @param maxiter Maximum number of iterations
   * @param trace Enable server-side tracing.
   * @return The absolute difference in error (between A and W*H) from the last iteration to the second-to-last.
   */
  public double NMF_Client(String Aorig, boolean transposeA,
                    String Wfinal, boolean transposeW, String Hfinal, boolean transposeH,
                    final int K, final int maxiter, boolean trace) {
    checkGiven(true, "Aorig", Aorig);
    Wfinal = emptyToNull(Wfinal);
    Hfinal = emptyToNull(Hfinal);
    Preconditions.checkArgument(Wfinal != null || Hfinal != null, "Either W or H must be given or the method is useless.");
    Preconditions.checkArgument(K > 0, "# of topics K must be > 0: " + K);
    deleteTables(false, Wfinal, Hfinal); // WTfinal, HTfinal

    // Scan A into memory
    Map<Key,Value> Aentries = new TreeMap<Key, Value>(); //GraphuloUtil.scanAll(connector, Aorig);
    OneTable(Aorig, null, null, Aentries, -1, null, null, null, null, null, null, null, false); // returns nnz A
    if (transposeA)
      Aentries = GraphuloUtil.transposeMap(Aentries);

    // Replace row and col labels with integer indexes; create map from indexes to original labels
    // The Maps are used to put the original labels on W and H
    SortedMap<Integer,String> rowMap = new TreeMap<Integer, String>(), colMap = new TreeMap<Integer, String>();
    RealMatrix Amatrix = MemMatrixUtil.indexMapAndMatrix(Aentries, rowMap, colMap);
    RealMatrix ATmatrix = Amatrix.transpose();

    int N = Amatrix.getRowDimension();
    int M = Amatrix.getColumnDimension();

    // Initialize W to a dense random matrix of size N x K
    RealMatrix Wmatrix = MemMatrixUtil.randNormPosFull(N, K);
    RealMatrix WTmatrix = Wmatrix.transpose(), Hmatrix, HTmatrix;

//    if (trace)
//      DebugUtil.printTable("0: W is NxK:", connector, Wfinal);

    // newerr starts at frobenius norm of A, since H starts at the zero matrix.
    double newerr = 0, olderr;
    int numiter = 0;

    do {
      numiter++;
      olderr = newerr;

      // H = ONLYPOS( (WT*W)^-1 * (WT*A) )
      //nmfStep(K, Wfinal, Aorig, Hfinal, HTfinal, Ttmp1, Ttmp2, trace);
      // assign to Hmatrix
      Hmatrix = nmfStep_Client(WTmatrix, Wmatrix, Amatrix);
      HTmatrix = Hmatrix.transpose();
//      if (trace)
//        DebugUtil.printTable(numiter + ": H is KxM:", connector, Hfinal);

      // WT = ONLYPOS( (H*HT)^-1 * (H*AT) )
      WTmatrix = nmfStep_Client(Hmatrix, HTmatrix, ATmatrix);
      Wmatrix = WTmatrix.transpose();
//      if (trace)
//        DebugUtil.printTable(numiter + ": W is NxK:", connector, Wfinal);

      newerr = Amatrix.subtract(Wmatrix.multiply(Hmatrix)).getFrobeniusNorm();
//      if (trace)
//        DebugUtil.printTable(numiter + ": A is NxM --- error is "+newerr+":", connector, Aorig);

      log.debug("NMF Iteration "+numiter+": olderr " + olderr + " newerr " + newerr);
    } while (Math.abs(newerr - olderr) > 0.01d && numiter < maxiter);

    // Write out results!
    if (Wfinal != null) {
      Map<Key, Value> Wmap = MemMatrixUtil.matrixToMapWithLabels(Wmatrix, rowMap, false); // false = label row
      GraphuloUtil.writeEntries(connector, transposeW ? GraphuloUtil.transposeMap(Wmap) : Wmap, Wfinal, true);
    }
    if (Hfinal != null) {
      Map<Key, Value> Hmap = MemMatrixUtil.matrixToMapWithLabels(Hmatrix, colMap, true); // true = label column qualifier
      GraphuloUtil.writeEntries(connector, transposeH ? GraphuloUtil.transposeMap(Hmap) : Hmap, Hfinal, true);
    }
    return Math.abs(newerr - olderr);
  }

  private RealMatrix nmfStep_Client(RealMatrix M1, RealMatrix M2, RealMatrix MA) {
    RealMatrix MR = MemMatrixUtil.doInverse(M1.multiply(M2)).multiply(M1.multiply(MA));
    // only keep positive entries
    MR.walkInOptimizedOrder(new RealMatrixChangingVisitor() {
      @Override
      public void start(int rows, int columns, int startRow, int endRow, int startColumn, int endColumn) {

      }

      @Override
      public double visit(int row, int column, double value) {
        return value > 0 ? value : 0;
      }

      @Override
      public double end() {
        return 0;
      }
    });
    return MR;
  }

  /**
   * Performs HT * (H*HT)^(-1). Stores result in a new table Rtable.
   * Used for the calculation a * HT * (H*HT)^(-1) = w,
   * given some new rows 'a' that we want to see in factored form w.
   *
   * @param Htable Input
   * @param HTtable Transpose of inpt
   * @param K # of topics
   * @param Rtable Output table (must not exist beforehand, though we could relax this)
   * @param forceDelete Forcibly delete temporary tables used if they happen to exist. If false, throws an exception if they exist.
   * @param trace Enable server-side tracing.
   */
  public void doHT_HHTinv(String Htable, String HTtable, int K, String Rtable, boolean forceDelete, boolean trace) {
    checkGiven(true, "Htable, HTtable", Htable, HTtable);
    checkGiven(false, "Rtable", Rtable);
    Preconditions.checkArgument(K > 0, "# of topics K must be > 0: " + K);
    deleteTables(false, Rtable);

    String Ttmp1;
    String tmpBaseName = Htable+"_NMF_";
    Ttmp1 = tmpBaseName+"tmp1";
    deleteTables(forceDelete, Ttmp1);

    // H*HT
    TableMult(HTtable, HTtable, Ttmp1, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(ScalarOp.TIMES, ScalarType.DOUBLE),
        MathTwoScalar.combinerSetting(PLUS_ITERATOR_BIGDECIMAL.getPriority(), null, ScalarOp.PLUS, ScalarType.DOUBLE),
        null, null, null, false, false, -1, false);

    log.info("AFTER H*HT");

    // Inverse
    try {
      connector.tableOperations().compact(Ttmp1, null, null,
          Collections.singletonList(InverseMatrixIterator.iteratorSetting(PLUS_ITERATOR_BIGDECIMAL.getPriority() + 1, K)),
          true, true); // blocks
    } catch (AccumuloSecurityException e) {
      log.error("problem while compacting "+Ttmp1+" to take the matrix inverse of H*HT", e);
      throw new RuntimeException(e);
    } catch (AccumuloException e) {
      log.error("problem while compacting "+Ttmp1+" to take the matrix inverse of H*HT", e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("crazy", e);
      throw new RuntimeException(e);
    }

    log.info("AFTER INVERSE");

    // HT * inv
    TableMult(Htable, Ttmp1, Rtable, null, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(ScalarOp.TIMES, ScalarType.DOUBLE),
        MathTwoScalar.combinerSetting(PLUS_ITERATOR_BIGDECIMAL.getPriority(), null, ScalarOp.PLUS, ScalarType.DOUBLE),
        null, null, null, false, false, -1, false);

    deleteTables(true, Ttmp1);
  }

}
