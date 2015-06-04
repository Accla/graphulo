package edu.mit.ll.graphulo.mult;

import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.TypedValueCombiner;

import edu.mit.ll.graphulo.IMultiplyOp;

/**
 * Decode values as Long objects, multiply and re-encode the result.
 * Used for EWiseX instead of TableMult because it uses the row of A.
 */
public class LongEWiseX implements IMultiplyOp {
  private static TypedValueCombiner.Encoder<Long> encoder = new LongCombiner.StringEncoder();

  @Override
  public Map.Entry<Key, Value> multiplyEntry(ByteSequence Mrow, ByteSequence ATcolF, ByteSequence ATcolQ,
                                             ByteSequence BcolF, ByteSequence BcolQ, Value ATval, Value Bval) {
    final Key k = new Key(Mrow.getBackingArray(), ATcolF.getBackingArray(),
        ATcolQ.getBackingArray(), EMPTY_BYTES, System.currentTimeMillis());
    final Value v = new Value(encoder.encode(
        encoder.decode(ATval.get()) * encoder.decode(Bval.get())
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
