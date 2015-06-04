package edu.mit.ll.graphulo.mult;

import edu.mit.ll.graphulo.IMultiplyOp;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;

import java.util.Map;

/**
 * Decode values as Long objects, take the max, and re-encode the result.
 */
public class MinMultiply implements IMultiplyOp {
  private static TypedValueCombiner.Encoder<Long> encoder = new LongCombiner.StringEncoder();

  @Override
  public Map.Entry<Key, Value> multiplyEntry(ByteSequence Mrow, ByteSequence ATcolF, ByteSequence ATcolQ,
                                             ByteSequence BcolF, ByteSequence BcolQ, Value ATval, Value Bval) {
    final Key k = new Key(ATcolQ.getBackingArray(), ATcolF.getBackingArray(),
        BcolQ.getBackingArray(), EMPTY_BYTES, System.currentTimeMillis());
    final Value v = new Value(encoder.encode(
        Math.min( encoder.decode(ATval.get()), encoder.decode(Bval.get()) )
    ));
    return new Map.Entry<Key, Value>() {
      @Override
      public Key getKey() {
        return k;
      }

      @Override
      public Value getValue() {
        return v;
      }

      @Override
      public Value setValue(Value value) {
        throw new UnsupportedOperationException();
      }
    };
  }
}