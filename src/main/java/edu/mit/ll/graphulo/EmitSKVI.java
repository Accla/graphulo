package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/** Augmented SKVI with new OutputCollector paradigm. */
interface EmitSKVI extends SortedKeyValueIterator<Key,Value> {
  /** Seek to the range as in {@link org.apache.accumulo.core.iterators.SortedKeyValueIterator#seek(org.apache.accumulo.core.data.Range, java.util.Collection, boolean)},
   * then emit entries to the outputcollector until no more to emit or until {@link OutputCollector#shouldStop()} returns false.
   */
  void seekEmit(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive,
                OutputCollector oc) throws IOException;

//  void nextEmit(OutputCollector oc) throws IOException;
}

/** Code in the middle of an iterator stack. */
class EmitCombiner implements EmitSKVI {

  /** Combine with + operation. */
  static class CombineCollector implements OutputCollector {
    private OutputCollector ocDownstream;
    public CombineCollector(OutputCollector ocDownstream) {
      this.ocDownstream = ocDownstream;
    }

    Key storedKey;
    Value storedValue;

    public void collect(Key k, Value v) throws IOException {
      if (storedKey == null) { // first collect call
        storedKey = k;
        storedValue = v;
      } else {
        // check if this is a new column or same column
        if (storedKey.equals(k, PartialKey.ROW_COLFAM_COLQUAL)) {
          // same key: combine
          Long tmp = Long.valueOf(storedValue.toString()) + Long.valueOf(v.toString());
          storedValue = new Value(tmp.toString().getBytes());
        } else {
          // different key: emit this one and store the new one.
          ocDownstream.collect(storedKey, storedValue);
          storedKey = k;
          storedValue = v;
        }
      }
    }

    /** Stop whenever the downstream OutputCollector wants to stop; no additional stopping criteria. */
    public boolean shouldStop() {
      return ocDownstream.shouldStop();
    }

    /** Emit the last entry if there is one. */
    public void close() throws IOException {
      // side note: not sure whether should check shouldStop() or not. Will make implementation work either way.
      if (!shouldStop() && storedKey != null)
        ocDownstream.collect(storedKey, storedValue);
    }
  }


  private SortedKeyValueIterator<Key, Value> source;
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
  }

  public void seekEmit(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive,
                       OutputCollector ocDownstream) throws IOException {
    OutputCollector ocThis = new CombineCollector(ocDownstream);
    if (source instanceof EmitSKVI) {
      ((EmitSKVI)source).seekEmit(range, columnFamilies, inclusive, ocThis);
    } else {
      // backward compatible with old SKVIs
      try {
        source.seek(range, columnFamilies, inclusive);
        while (source.hasTop() && !ocThis.shouldStop()) {
          ocThis.collect(source.getTopKey(), source.getTopValue());
          source.next();
        }
      } catch (IOException e) {
        // drop source
      }
    }
  }

  // --- legacy implementation ---
  CombineCollector ocThis;
  static final OutputCollector doNothingCollector = new OutputCollector() {
    public void collect(Key k, Value v) {}
    public boolean shouldStop() { return false; }
    public void close() {}
  };

  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    source.seek(range, columnFamilies, inclusive);
    ocThis = new CombineCollector(doNothingCollector);
  }

  public void next() throws IOException {

  }

  public boolean hasTop() {
    return ocThis.storedKey != null;
  }


  public Key getTopKey() {
    return ocThis.storedKey;
  }

  public Value getTopValue() {
    return ocThis.storedValue;
  }

  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    EmitCombiner copy = new EmitCombiner();
    copy.ocThis = new CombineCollector(doNothingCollector);
    copy.ocThis.storedKey = ocThis.storedKey;
    copy.ocThis.storedValue = ocThis.storedValue;
    return copy;
  }
} // end of class EmitCombiner



//class EmitSourceSwitchingIterator {
//
//}












// original
///** Similar to org.apache.hadoop.mapred.OutputCollector<Key,Value> */
//interface OutputCollector {
//  /**
//   * Add an entry to output.
//   */
//  void collect(Key k, Value v);
//}
//
//interface StopSignaler {
//  /**
//   * @return true if the calling iterator should stop emitting values.
//   */
//  boolean shouldStop();
//}
//
//interface Emitter {
//  void doEmitting(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive,
//                  OutputCollector oc, StopSignaler sc);
//}
