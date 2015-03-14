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
     * <p>
     * If Ptable already exists, then sums in entries from P.
     * If P==C, then does not open an extra reader to Ctable.
     * <p>
     * Postcondition: Ptable is created and ready for scanning.
     *
     * @param Ptable Name of the "stored procedure table" to hold computation iterators.
     * @param Atable Name of Accumulo table holding matrix A.
     * @param BTtable Name of Accumulo table holding matrix transpose(B).
     * @param Ctable Optional. A table we add to the result of A (*.+) B.
     * @param Rtable Optional, assumed P if not given. Table to store results.
     * @param multOp An operation that "multiplies" two values.
     * @param sumOp  An operation that "sums" values.
     * @param rowFilter Optional. Row subset of Atable and BTtable.
     * @param colFilter Optional. Column family/qualifier subset of A and BT.
     *                  A variant takes a collection of ranges. This is less efficient since
     *                  {@link org.apache.accumulo.core.client.ScannerBase#fetchColumn} and fetchColumnFamily
     *                  do not take ranges (meaning, need to scan all the columns and filter after receiving,
     *                  or use a {@link org.apache.accumulo.core.iterators.Filter}). Row subsets are easier than column.
     */
    public void TableMult(String Ptable,
                       String Atable, String BTtable,
                       Class<? extends IMultiplyOp> multOp, Class<? extends Combiner> sumOp,
                       Collection<Range> rowFilter,
                       Collection<IteratorSetting.Column> colFilter,
                       String Ctable, String Rtable);



}
