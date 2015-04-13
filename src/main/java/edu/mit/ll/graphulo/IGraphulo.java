package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.Combiner;

import java.util.Collection;

/**
 * Graphulo interface
 */
public interface IGraphulo {

  /**
   * Create and execute the stored procedure table Ptable to compute C += A * B.
   * User-defined "plus" and "multiply". Requires transpose table AT instead of A.
   * If C is not given, then the scan itself returns the results of A * B.
   *
   * @param ATtable   Name of Accumulo table holding matrix transpose(A).
   * @param Btable    Name of Accumulo table holding matrix B.
   * @param Ctable    Optional. Name of table to store result. Streams back result if null.
   * @param multOp    An operation that "multiplies" two values.
   * @param sumOp     An operation that "sums" values.
   * @param rowFilter Optional. Row subset of ATtable and Btable.
   * @param colFilter Optional. Column family/qualifier subset of AT and B.
   *                  A variant takes a collection of ranges. This is less efficient since
   *                  {@link org.apache.accumulo.core.client.ScannerBase#fetchColumn} and fetchColumnFamily
   *                  do not take ranges (meaning, need to scan all the columns and filter after receiving,
   *                  or use a {@link org.apache.accumulo.core.iterators.Filter}). Row subsets are easier than column.
   */
  void TableMult(String ATtable, String Btable, String Ctable,
                 Class<? extends IMultiplyOp> multOp, Class<? extends Combiner> sumOp,
                 Collection<Range> rowFilter,
                 Collection<IteratorSetting.Column> colFilter);

}
