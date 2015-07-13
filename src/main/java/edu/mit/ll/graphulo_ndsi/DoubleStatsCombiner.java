package edu.mit.ll.graphulo_ndsi;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;

import java.util.Iterator;

/**
 * Like {@link } except acts on double Values instead of longs.
 */
public class DoubleStatsCombiner extends Combiner {
  @Override
  public Value reduce(Key key, Iterator<Value> iter) {
    return null;
  }
}
