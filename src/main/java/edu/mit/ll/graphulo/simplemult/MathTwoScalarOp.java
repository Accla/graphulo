package edu.mit.ll.graphulo.simplemult;

import edu.mit.ll.graphulo.apply.ApplyIterator;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Math operations between two scalars.
 * Can be used as an ApplyOp by setting one of the sides to a constant scalar.
 */
public class MathTwoScalarOp extends SimpleTwoScalarOp {
  private static final Logger log = LogManager.getLogger(MathTwoScalarOp.class);

  public enum ScalarOp {
    PLUS, TIMES, SET_LEFT, MINUS,
    DIVIDE, POWER, MIN, MAX
  }
  public enum ScalarType {
    LONG, DOUBLE, BIGDECIMAL
  }

  public static final String
      SCALAR_OP = "ScalarOp",
      SCALAR_TYPE = "scalarType";

  /** For use as an ApplyOp.
   * Create an IteratorSetting that performs a ScalarOp on every Value it sees, parsing Values as Doubles. */
  public static IteratorSetting applyOpDouble(int priority, boolean onRight, ScalarOp op, double scalar) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, MathTwoScalarOp.class.getName());
    for (Map.Entry<String, String> entry : optionMap(op, ScalarType.DOUBLE).entrySet())
      itset.addOption(ApplyIterator.APPLYOP + ApplyIterator.OPT_SUFFIX + entry.getKey(), entry.getValue());
    itset = SimpleTwoScalarOp.addOptionsToIteratorSetting(itset, onRight, new Value(Double.toString(scalar).getBytes()));
    return itset;
  }

  /** For use as an ApplyOp.
   * Create an IteratorSetting that performs a ScalarOp on every Value it sees, parsing Values as Longs. */
  public static IteratorSetting applyOpLong(int priority, boolean onRight, ScalarOp op, long scalar) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, MathTwoScalarOp.class.getName());
    for (Map.Entry<String, String> entry : optionMap(op, ScalarType.LONG).entrySet())
      itset.addOption(ApplyIterator.APPLYOP + ApplyIterator.OPT_SUFFIX + entry.getKey(), entry.getValue());
    itset = SimpleTwoScalarOp.addOptionsToIteratorSetting(itset, onRight, new Value(Long.toString(scalar).getBytes()));
    return itset;
  }

  /** For use as an ApplyOp.
   * Create an IteratorSetting that performs a ScalarOp on every Value it sees, parsing Values as BigDecimal objects. */
  public static IteratorSetting applyOpBigDecimal(int priority, boolean onRight, ScalarOp op, BigDecimal scalar) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, MathTwoScalarOp.class.getName());
    for (Map.Entry<String, String> entry : optionMap(op, ScalarType.BIGDECIMAL).entrySet())
      itset.addOption(ApplyIterator.APPLYOP + ApplyIterator.OPT_SUFFIX + entry.getKey(), entry.getValue());
    itset = SimpleTwoScalarOp.addOptionsToIteratorSetting(itset, onRight, new Value(scalar.toString().getBytes())); // byte encoding UTF-8?
    return itset;
  }

  /** For use as a Combiner. Pass columns as null or empty to combine on all columns. */
  public static IteratorSetting combinerSetting(int priority, List<IteratorSetting.Column> columns, ScalarOp op, ScalarType type) {
    IteratorSetting itset = new IteratorSetting(priority, MathTwoScalarOp.class);
    if (columns == null || columns.isEmpty())
      Combiner.setCombineAllColumns(itset, true);
    else
      Combiner.setColumns(itset, columns);
    itset.addOptions(optionMap(op, type));
    return itset;
  }

  public static Map<String,String> optionMap(ScalarOp op, ScalarType type) {
    Map<String,String> map = new HashMap<>();
    map.put(SCALAR_OP, op.name());
    map.put(SCALAR_TYPE, type.name());
    return map;
  }
  

  private ScalarType scalarType = ScalarType.BIGDECIMAL; // default
  private ScalarOp scalarOp = ScalarOp.TIMES;  // default

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    Map<String,String> extraOpts = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String k = entry.getKey(), v = entry.getValue();
      switch (k) {
        case SCALAR_TYPE:
          scalarType = ScalarType.valueOf(options.get(SCALAR_TYPE));
          break;
        case SCALAR_OP: scalarOp = ScalarOp.valueOf(v); break;
        default:
          extraOpts.put(k, v);
          break;
      }
    }
    super.init(extraOpts, env);
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public Value multiply(Value Aval, Value Bval) {
    if (scalarOp == ScalarOp.SET_LEFT) {
      return Aval;
    }

    Number Anum, Bnum;
    switch(scalarType) {
      case LONG:
        Anum = Long.valueOf(new String(Aval.get()));
        Bnum = Long.valueOf(new String(Bval.get()));
        break;
      case DOUBLE:
        Anum = Double.valueOf(new String(Aval.get()));
        Bnum = Double.valueOf(new String(Bval.get()));
        break;
      case BIGDECIMAL:
        Anum = new BigDecimal(new String(Aval.get()));
        Bnum = new BigDecimal(new String(Bval.get()));
        break;
      default: throw new AssertionError();
    }
    Number nnew;
    switch(scalarOp) {
      case PLUS:
        switch(scalarType) {
          case LONG: nnew = Anum.longValue() + Bnum.longValue(); break;
          case DOUBLE: nnew = Anum.doubleValue() + Bnum.doubleValue(); break;
          case BIGDECIMAL: nnew = ((BigDecimal)Anum).add((BigDecimal)Bnum); break;
          default: throw new AssertionError();
        }
        break;
      case TIMES:
        switch(scalarType) {
          case LONG: nnew = Anum.longValue() * Bnum.longValue(); break;
          case DOUBLE: nnew = Anum.doubleValue() * Bnum.doubleValue(); break;
          case BIGDECIMAL: nnew = ((BigDecimal)Anum).multiply((BigDecimal)Bnum); break;
          default: throw new AssertionError();
        }
        break;
      case MINUS:
        switch(scalarType) {
          case LONG: nnew = Anum.longValue() - Bnum.longValue(); break;
          case DOUBLE: nnew = Anum.doubleValue() - Bnum.doubleValue(); break;
          case BIGDECIMAL: nnew = ((BigDecimal)Anum).subtract((BigDecimal) Bnum); break;
          default: throw new AssertionError();
        }
        break;
      case DIVIDE:
        switch(scalarType) {
          case LONG: nnew = Anum.longValue() / Bnum.longValue(); break;
          case DOUBLE: nnew = Anum.doubleValue() / Bnum.doubleValue(); break;
          case BIGDECIMAL: nnew = ((BigDecimal)Anum).divide((BigDecimal) Bnum, BigDecimal.ROUND_HALF_UP); break;
          default: throw new AssertionError();
        }
        break;
      case POWER:
        switch(scalarType) {
          case LONG: nnew = (long)Math.pow(Anum.longValue(), Bnum.longValue()); break;
          case DOUBLE: nnew = Math.pow(Anum.doubleValue(), Bnum.doubleValue()); break;
          case BIGDECIMAL: nnew = ((BigDecimal)Anum).pow(Bnum.intValue()); break;
          default: throw new AssertionError();
        }
        break;
      case MIN:
        switch(scalarType) {
          case LONG: nnew = Math.min(Anum.longValue(), Bnum.longValue()); break;
          case DOUBLE: nnew = Math.min(Anum.doubleValue(), Bnum.doubleValue()); break;
          case BIGDECIMAL: nnew = ((BigDecimal)Anum).min((BigDecimal) Bnum); break;
          default: throw new AssertionError();
        }
        break;
      case MAX:
        switch(scalarType) {
          case LONG: nnew = Math.max(Anum.longValue(), Bnum.longValue()); break;
          case DOUBLE: nnew = Math.max(Anum.doubleValue(), Bnum.doubleValue()); break;
          case BIGDECIMAL: nnew = ((BigDecimal)Anum).max((BigDecimal) Bnum); break;
          default: throw new AssertionError();
        }
        break;
      default:
        throw new AssertionError();
    }
    Value vnew;
    switch(scalarType) {
      case LONG: vnew = new Value(Long.toString(nnew.longValue()).getBytes()); break;
      case DOUBLE: vnew = new Value(Double.toString(nnew.doubleValue()).getBytes()); break;
      case BIGDECIMAL: vnew = new Value(nnew.toString().getBytes()); break;
      default: throw new AssertionError();
    }
    return vnew;
  }
}
