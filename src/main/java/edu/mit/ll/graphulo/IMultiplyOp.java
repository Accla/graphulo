package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Iterator;
import java.util.Map;

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
