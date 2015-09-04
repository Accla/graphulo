package edu.mit.ll.graphulo.apply;

import edu.mit.ll.graphulo.skvi.TwoTableIterator;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Apply a function to each entry. Can generate zero, one, or more entries per apply.
 */
public interface ApplyOp {
  /**
   * Initializes the apply object.
   * Options are passed from <tt>multiplyOp.opt.OPTION_NAME</tt> in the options for {@link TwoTableIterator}.
   *
   * @param options
   *          <tt>Map</tt> map of string option names to option values.
   * @param env
   *          <tt>IteratorEnvironment</tt> environment in which iterator is being run.
   * @exception IllegalArgumentException
   *              if there are problems with the options.
   */
  void init(Map<String,String> options, IteratorEnvironment env) throws IOException;


  /**
   * The function to apply.
   * If modifying the row portion of the Key, be careful to stay within the seek range unless you know what you are doing.
   * @return Iterator over result of applying the function this class represents to an entry. Use {@link Collections#emptyIterator()} if no entries to emit.
   */
  Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v) throws IOException;

  /** Passes the seek range to the applyOp, in case knowing the seek range information is useful.
   * This will be called before any {@link #apply}. */
  void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException;
}
