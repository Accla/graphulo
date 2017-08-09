package edu.mit.ll.graphulo.rowmult;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.SortedMap;

/**
 * Extends MultiplyOp to allow state initalization at the beginning of a row.
 */
public interface RowStartMultiplyOp extends MultiplyOp {

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
   * @param isCollision True if about to be called with elements for both A and B. False for the AA or BB case.
   * @throws UnsupportedOperationException If called in an unsupported mode.
   * @return  True to proceed. False to skip the entire matching rows.
   *          False also skips the "alsoDoAA" and alsoDoBB" on the row if applicable.
   */
  boolean startRow(SortedMap<Key,Value> mapRowA, SortedMap<Key,Value> mapRowB, boolean isCollision);
}
