package edu.mit.ll.graphulo.apply;

import edu.mit.ll.graphulo.skvi.TwoTableIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Apply a function to each entry. Can generate zero, one or more entries per apply.
 */
public interface ApplyOp {
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
   * The function to apply.
   * @return Iterator over result of multiplying the two entries. Use {@link Collections#emptyIterator()} if no entries to emit.
   */
  Iterator<? extends Map.Entry<Key, Value>> multiply(Key k, Value v);
}
