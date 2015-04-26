package edu.mit.ll.graphulo.mult;

import edu.mit.ll.graphulo.IMultiplyOp;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Decode values as BigDecimal objects, multiply and re-encode the result.
 */
public class BigDecimalMultiply implements IMultiplyOp {
    private static TypedValueCombiner.Encoder<BigDecimal> encoder = new BigDecimalCombiner.BigDecimalEncoder();

    @Override
    public Map.Entry<Key, Value> multiplyEntry(ByteSequence Mrow, ByteSequence ATcolF, ByteSequence ATcolQ,
                                               ByteSequence BcolF, ByteSequence BcolQ, Value ATval, Value Bval) {
        final Key k = new Key(ATcolQ.getBackingArray(), ATcolF.getBackingArray(),
            BcolQ.getBackingArray(), EMPTY_BYTES, System.currentTimeMillis());
        final Value v = new Value(encoder.encode(
            encoder.decode(ATval.get()).multiply( encoder.decode(Bval.get()) )
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
