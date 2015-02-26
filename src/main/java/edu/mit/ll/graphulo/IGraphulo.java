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
     * Create and execute the stored procedure table Ptable
     * to compute R := C + A (*.+) B.
     * A (*.+) B means A "times" B with user-defined "plus" and "multiply".
     *
     * @param Ptable Name of the "stored procedure table" to hold computation iterators.
     * @param Atable Name of Accumulo table holding matrix A.
     * @param Arows  Optional. Row subset of Atable.
     * @param BTtable Name of Accumulo table holding matrix transpose(B).
     * @param BTrows Optional. Row subset of BTtable.
     * @param Ctable Optional. A table we add to the result of A (*.+) B.
     * @param Rtable Table to store the result.
     * @param multOp An operation that "multiplies" two values.
     * @param sumOp  An operation that "sums" values.
     * @param colFilter Optional. Column family/qualifier subset of A and BT.
     *                  A variant takes a collection of ranges. This is less efficient since
     *                  {@link org.apache.accumulo.core.client.ScannerBase#fetchColumn} and fetchColumnFamily
     *                  do not take ranges (meaning, need to scan all the columns and filter after receiving,
     *                  or use a {@link org.apache.accumulo.core.iterators.Filter}). Row subsets are easier than column.
     */
    public void SpGEMM(String Ptable,
                       String Atable, Collection<Range> Arows,
                       String BTtable, Collection<Range> BTrows,
                       String Ctable, String Rtable,
                       IMultiplyOp multOp, Combiner sumOp,
                       Collection<IteratorSetting.Column> colFilter);



}
