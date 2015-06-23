package edu.mit.ll.graphulo.ewise;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A simple abstract class for element0-wise multiplication
 * that returns zero or one entry per multiply.
 */
public abstract class SimpleEWiseX implements EWiseOp {

  /** Implements simple multiply logic. Returning null means no entry is emitted. */
  public abstract Value multiply(Value Aval, Value Bval);

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
  }

  @Override
  public Iterator<? extends Entry<Key, Value>> multiply(ByteSequence Mrow, ByteSequence ATcolF, ByteSequence ATcolQ, Value ATval, Value Bval) {
    if (ATval == null || Bval == null)
      return Collections.<Map.Entry<Key,Value>>emptyIterator();
    Key k = new Key(Mrow.getBackingArray(), ATcolF.getBackingArray(),
        ATcolQ.getBackingArray(), GraphuloUtil.EMPTY_BYTES, System.currentTimeMillis());
    Value v = multiply(ATval, Bval);
    return v == null ? Collections.<Map.Entry<Key,Value>>emptyIterator() : Iterators.singletonIterator((Map.Entry<Key, Value>) new AbstractMap.SimpleImmutableEntry<>(k, v));
  }
}
