package edu.mit.ll.graphulo.rowmult;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Used for Incidence Table Breadth First Search.
 */
public class EdgeBFSMultiply implements MultiplyOp, Iterator<Map.Entry<Key,Value>> {
  private static final Logger log = LogManager.getLogger(EdgeBFSMultiply.class);

  public static final String NEW_VISIBILITY = "newVisibility";

//  private Text outColumnPrefix, inColumnPrefix;
  private byte[] newVisibility = new byte[0];

//  private enum FILTER_MODE { NONE, SIMPLE, RANGES }
//  private Collection<Text> simpleFilter;
//  private Collection<Range> rangesFilter;

  private Value emitValueFirst, emitValueSecond;
  private Key emitKeyFirst, emitKeySecond;

  private void parseOptions(Map<String,String> options) {
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String v = entry.getValue();
      switch (entry.getKey()) {
//        case "outColumnPrefix": outColumnPrefix = new Text(v); break;
//        case "inColumnPrefix": inColumnPrefix = new Text(v); break;
        case NEW_VISIBILITY: newVisibility = v.getBytes(StandardCharsets.UTF_8); break;
        default:
          log.warn("Unrecognized option: " + entry);
          break;
      }
    }

  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    parseOptions(options);
  }


  @Override
  public Iterator<? extends Map.Entry<Key, Value>> multiply(ByteSequence Mrow, ByteSequence ATcolF, ByteSequence ATcolQ, ByteSequence BcolF, ByteSequence BcolQ,
                                                            Value ATval, Value Bval) {
    // maybe todo: check whether ATcolQ is of the form "out|v0"
//    if (ATcolQ.length() < outColumnPrefix.getLength() ||
//        0 != WritableComparator.compareBytes(ATcolQ.getBackingArray(), 0, outColumnPrefix.getLength(), outColumnPrefix.getBytes(), 0, outColumnPrefix.getLength())) {
//      emitKeyFirst = emitKeySecond = null;
//      return;
//    }
    emitKeyFirst = new Key(Mrow.getBackingArray(), ATcolF.getBackingArray(), ATcolQ.getBackingArray(),
        newVisibility, System.currentTimeMillis()); // experiment with copy=false?
    emitKeySecond = new Key(Mrow.getBackingArray(), BcolF.getBackingArray(), BcolQ.getBackingArray(),
        newVisibility, System.currentTimeMillis()); // experiment with copy=false?
    emitValueFirst = new Value(ATval);
    emitValueSecond = new Value(Bval);
    return this;
  }

  @Override
  public boolean hasNext() {
    return emitKeyFirst != null;
  }

  @Override
  public Map.Entry<Key, Value> next() {
    Key emitK = emitKeyFirst;
    emitKeyFirst = emitKeySecond;
    emitKeySecond = null;
    Value emitV = emitValueFirst;
    emitValueFirst = emitValueSecond;
    emitValueSecond = null;
//    if (emitKeyFirst == null)
//      emitValueFirst = null;
    return new AbstractMap.SimpleImmutableEntry<>(emitK, emitV);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
