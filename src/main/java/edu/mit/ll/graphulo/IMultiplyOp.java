package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;

import java.math.BigDecimal;

/**
 * A multiplication operation on 2 Values. Result is a Value.
 * Will respect order for non-commutative operators.
 */
public interface IMultiplyOp {
    /**
     * A multiplication operation on 2 Values. Result is a Value.
     * Will respect order for non-commutative operators.
     */
    public Value multiply(Value v1, Value v2);
}
