package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.skvi.LruCacheIterator;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Contains convenience functions for calling Graphulo functions from Matlab.
 * Matlab works best when the arguments are all primitive types and strings.
 */
@SuppressWarnings("unused") // Used in Matlab
public class MatlabGraphulo extends Graphulo {
  private static final Logger log = LogManager.getLogger(MatlabGraphulo.class);

  static {
    // load log4j once, when this class is loaded
    DOMConfigurator.configure(MatlabGraphulo.class.getClassLoader().getResource("log4j.xml"));
  }

  public MatlabGraphulo(String instanceName, String zookeepers, String username, String password)
      throws AccumuloSecurityException, AccumuloException {
    super(new ZooKeeperInstance(instanceName, zookeepers).getConnector(username, new PasswordToken(password)), new PasswordToken(password));
  }

  public long TableMult(String ATtable, String Btable, String Ctable) {
    return TableMult(ATtable, Btable, Ctable, -1, true);
  }

  public long TableMult(String ATtable, String Btable, String Ctable, String rowFilter, String colFilterAT, String colFilterB) {
    return TableMult(ATtable, Btable, Ctable, rowFilter, colFilterAT, colFilterB, -1, true);
  }

  public long TableMult(String ATtable, String Btable, String Ctable, int numEntriesCheckpoint, boolean trace) {
    return TableMult(ATtable, Btable, Ctable, null, null, null, numEntriesCheckpoint, true);
  }

  public long TableMult(String ATtable, String Btable, String Ctable,
                        String rowFilter, String colFilterAT, String colFilterB,
                        int numEntriesCheckpoint, boolean trace) {
    return TableMult(ATtable, Btable, Ctable, null,
        rowFilter, colFilterAT, colFilterB, numEntriesCheckpoint, trace);
  }

  public long TableMult(String ATtable, String Btable, String Ctable, String CTtable,
                        String rowFilter, String colFilterAT, String colFilterB,
                        int numEntriesCheckpoint, boolean trace) {
    return TableMult(ATtable, Btable, Ctable, null,
        rowFilter, colFilterAT, colFilterB, -1, numEntriesCheckpoint, trace);
  }

  public long TableMult(String ATtable, String Btable, String Ctable, String CTtable,
                        String rowFilter, String colFilterAT, String colFilterB,
                        int presumCacheSize,
                        int numEntriesCheckpoint, boolean trace) {
    Collection<Range> rowFilterRanges =
        rowFilter != null && !rowFilter.isEmpty() ? GraphuloUtil.d4mRowToRanges(rowFilter) : null;
    List<IteratorSetting> itAfterTT = Collections.singletonList(LruCacheIterator.combinerSetting(
        1, null, presumCacheSize, MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.PLUS, MathTwoScalar.ScalarType.LONG, "", false)
    ));

    return TableMult(ATtable, Btable, Ctable, CTtable, -1,
        MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG, "", false), Graphulo.PLUS_ITERATOR_LONG,
        rowFilterRanges, colFilterAT, colFilterB, false, false, null, null, itAfterTT, null, null, numEntriesCheckpoint, Authorizations.EMPTY, Authorizations.EMPTY);
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

  /** Full major compact a table and wait for it to finish. */
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

  /** Flush a table, writing entries in memory to disk. */
  public void Flush(String table) {
    System.out.println("Flushing " + table + "...");
    try {
      connector.tableOperations().flush(table, null, null, true);
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.error("error trying to compact " + table, e);
    } catch (TableNotFoundException e) {
      log.error("", e);
    }
  }

  public boolean Exists(String table) {
    return connector.tableOperations().exists(table);
  }



}
