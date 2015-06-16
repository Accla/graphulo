package edu.mit.ll.graphulo.mult;

import edu.mit.ll.graphulo.skvi.TwoTableIterator;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * Multiplication operation on 2 entries. Respects order for non-commutative operations.
 * Returns any number of entries as a result of the multiplication.
 */
public interface IMultiplyOp extends Iterator<Map.Entry<Key,Value>> {
  /**
   * Useful for ignoring column visibility.
   */
  byte EMPTY_BYTES[] = new byte[0];


  /**
   * Initializes the multiply object.
   * Options are passed from <tt>multiplyOp.opt.OPTION_NAME</tt> in the options for {@link TwoTableIterator}.
   *
   * @param options
   *          <tt>Map</tt> map of string option names to option values.
   * @param env
   *          <tt>IteratorEnvironment</tt> environment in which iterator is being run.
   * @throws IOException
   *           unused.
   * @exception IllegalArgumentException
   *              if there are problems with the options.
   */
  void init(Map<String,String> options, IteratorEnvironment env) throws IOException;

  /**
   * A method that signals the start of matching rows in TwoTableIterator.
   * Depending on the mode of TwoTableIterator,
   * this will be called before processing matching rows form the two tables..
   *
   * <ol>
   * <li>TWOROW mode: passes both maps.</li>
   * <li>ONEROWA mode: passes mapRowA, passes null for mapRowB</li>
   * <li>ONEROWB mode: passes null for mapRowA, passes mapRowB</li>
   * <li>EWISE and NONE mode: does not call this method</li>
   * </ol>
   *
   * An implementing class should perform any state setup that requires holding entire rows in memory.
   *
   * @param mapRowA Sorted map of entries held in memory for table A
   * @param mapRowB Sorted map of entries held in memory for table A
   * @throws UnsupportedOperationException If called in an unsupported mode.
   */
  void startRow(SortedMap<Key,Value> mapRowA, SortedMap<Key,Value> mapRowB);


  /**
   * Multiplication operation on 2 entries with matching rows.
   * In the case of TableMult, the 2 entries are from table AT and B in the outer product.
   * In the case of SpEWise, the 2 entries match on row and column.
   * <p/>
   * NOTE: This call should "reset" the class, such that it stops iterating over any previous entries.
   * Returned Keys and Values should be newly allocated.
   *
   * @param Mrow   Pointer to data for matching row. Do not modify.
   * @param ATcolF Pointer to data for AT column family. Do not modify.
   * @param ATcolQ Pointer to data for AT column qualifier. Do not modify.
   * @param BcolF  Pointer to data for B column family. Do not modify.
   * @param BcolQ  Pointer to data for B column qualifier. Do not modify.
   * @param ATval  Pointer to data for AT value. Do not modify.
   * @param Bval   Pointer to data for B value. Do not modify.
   */
  void multiply(ByteSequence Mrow, ByteSequence ATcolF, ByteSequence ATcolQ,
                ByteSequence BcolF, ByteSequence BcolQ, Value ATval, Value Bval);
}
