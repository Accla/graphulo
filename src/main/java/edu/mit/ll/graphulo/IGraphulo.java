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
   * C += A * B.
   * User-defined "plus" and "multiply". Requires transpose table AT instead of A.
   * If C is not given, then the scan itself returns the results of A * B.
   *
   * @param ATtable   Name of Accumulo table holding matrix transpose(A).
   * @param Btable    Name of Accumulo table holding matrix B.
   * @param Ctable    Optional. Name of table to store result. Streams back result if null.
   * @param multOp    An operation that "multiplies" two values.
   * @param sumOp     An operation that "sums" values.
   * @param rowFilter Optional. Row subset of ATtable and Btable, like "a,:,b,g,c,:,"
   * @param colFilterAT Optional. Column qualifier subset of AT, currently restricted to not allow ranges.
   * @param colFilterB Optional. Column qualifier subset of B, like "a,f,b,c,"
   */
  void TableMult(String ATtable, String Btable, String Ctable,
                 Class<? extends IMultiplyOp> multOp, Class<? extends Combiner> sumOp,
                 Collection<Range> rowFilter,
                 String colFilterAT, String colFilterB);

}
