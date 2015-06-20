package edu.mit.ll.graphulo.ewise;

import edu.mit.ll.graphulo.skvi.TwoTableIterator;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Element-wise Multiplication on 2 entries. Respects order for non-commutative operations.
 * Iterates through any number of entries as a result of the multiplication.
 */
public interface EWiseOp {

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
   * Multiplication operation on 2 entries with matching row, column family and column qualifier.
   *
   * @param Mrow  Pointer to data for matching row. Do not modify.
   * @param McolF Pointer to data for matching column family. Do not modify.
   * @param McolQ Pointer to data for matching column qualifier. Do not modify.
   * @param Aval  Pointer to data for AT value. Do not modify.
   * @param Bval  Pointer to data for B value. Do not modify.
   * @return Iterator over result of multiplying the two entries. Use {@link Collections#emptyIterator()} if no entries to emit.
   */
  Iterator<? extends Map.Entry<Key, Value>> multiply(ByteSequence Mrow, ByteSequence McolF, ByteSequence McolQ,
                                                     Value Aval, Value Bval);

}
