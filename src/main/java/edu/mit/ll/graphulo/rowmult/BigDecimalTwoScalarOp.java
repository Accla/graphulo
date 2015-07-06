package edu.mit.ll.graphulo.rowmult;

import edu.mit.ll.graphulo.simplemult.SimpleTwoScalarOp;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;

import java.math.BigDecimal;

/**
 * Decode values as BigDecimal objects, multiply and re-encode the result.
 */
public class BigDecimalTwoScalarOp extends SimpleTwoScalarOp {
    private static final TypedValueCombiner.Encoder<BigDecimal> encoder = new BigDecimalCombiner.BigDecimalEncoder();

    @Override
    public Value multiply(Value ATval, Value Bval) {
        return new Value(encoder.encode(
            encoder.decode(ATval.get()).multiply( encoder.decode(Bval.get()) )
        ));
    }
}
