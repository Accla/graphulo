package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Multiplication operation on 2 entries from table AT and B that match on their row in the outer product.
 * Respects order for non-commutative operations.
 */
public interface IMultiplyOp {
  /** Useful for ignoring column visibility. */
  byte EMPTY_BYTES[] = new byte[0];

  /**
   * Multiplication operation on 2 entries from table AT and B that match on their row in the outer product.
   * @param Mrow Pointer to data for matching row. Do not modify.
   * @param ATcolF Pointer to data for AT column family. Do not modify.
   * @param ATcolQ Pointer to data for AT column qualifier. Do not modify.
   * @param BcolF Pointer to data for B column family. Do not modify.
   * @param BcolQ Pointer to data for B column qualifier. Do not modify.
   * @param ATval Pointer to data for AT value. Do not modify.
   * @param Bval Pointer to data for B value. Do not modify.
   * @return Newly allocated Key and Value. Or return null to indicate the result is a zero.
   */
  Map.Entry<Key, Value> multiplyEntry(ByteSequence Mrow, ByteSequence ATcolF, ByteSequence ATcolQ,
                                      ByteSequence BcolF, ByteSequence BcolQ, Value ATval, Value Bval);
}
