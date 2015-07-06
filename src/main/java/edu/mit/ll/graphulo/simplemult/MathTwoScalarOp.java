package edu.mit.ll.graphulo.simplemult;

import edu.mit.ll.graphulo.apply.ApplyIterator;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Math operations between two scalars.
 * Can be used as an ApplyOp by setting one of the sides to a constant scalar.
 */
public class MathTwoScalarOp extends SimpleTwoScalarOp {
  private static final Logger log = LogManager.getLogger(MathTwoScalarOp.class);

  public enum ScalarOp {
    PLUS, TIMES, SET_LEFT, MINUS,
    DIVIDE, POWER
  }
  public enum ScalarType {
    LONG, DOUBLE
  }

  public static final String
      SCALAR_OP = "ScalarOp",
      SCALAR_TYPE = "scalarType";

  /** For use as an ApplyOp.
   * Create an IteratorSetting that performs a ScalarOp on every Value it sees, parsing Values as Doubles. */
  public static IteratorSetting iteratorSettingDouble(int priority, boolean onRight, ScalarOp op, double scalar) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, MathTwoScalarOp.class.getName());
    itset.addOption(ApplyIterator.APPLYOP+ApplyIterator.OPT_SUFFIX+SCALAR_OP, op.name());
    itset.addOption(ApplyIterator.APPLYOP+ApplyIterator.OPT_SUFFIX+SCALAR_TYPE, ScalarType.DOUBLE.name());
    itset = SimpleTwoScalarOp.addOptionsToIteratorSetting(itset, onRight, new Value(Double.toString(scalar).getBytes()));
    return itset;
  }

  /** For use as an ApplyOp.
   * Create an IteratorSetting that performs a ScalarOp on every Value it sees, parsing Values as Longs. */
  public static IteratorSetting iteratorSettingLong(int priority, boolean onRight, ScalarOp op, long scalar) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, MathTwoScalarOp.class.getName());
    itset.addOption(ApplyIterator.APPLYOP+ApplyIterator.OPT_SUFFIX+SCALAR_OP, op.name());
    itset.addOption(ApplyIterator.APPLYOP+ApplyIterator.OPT_SUFFIX+SCALAR_TYPE, ScalarType.LONG.name());
    itset = SimpleTwoScalarOp.addOptionsToIteratorSetting(itset, onRight, new Value(Long.toString(scalar).getBytes()));
    return itset;
  }

  // I want developers to get used to writing out the usual signatures.
//  /** Shortcut method for common Abs0 op */
//  public static IteratorSetting abs0Apply(int priority) {
//    return iteratorSettingLong(priority, 1, ScalarOp.SET_LEFT);
//  }

  private ScalarType scalarType = ScalarType.DOUBLE;
  private ScalarOp scalarOp;

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
      default: throw new AssertionError();
    }
    Number nnew;
    switch(scalarOp) {
      case PLUS:
        switch(scalarType) {
          case LONG: nnew = Anum.longValue() + Bnum.longValue(); break;
          case DOUBLE: nnew = Anum.doubleValue() + Bnum.doubleValue(); break;
          default: throw new AssertionError();
        }
        break;
      case TIMES:
        switch(scalarType) {
          case LONG: nnew = Anum.longValue() * Bnum.longValue(); break;
          case DOUBLE: nnew = Anum.doubleValue() * Bnum.doubleValue(); break;
          default: throw new AssertionError();
        }
        break;
      case MINUS:
        switch(scalarType) {
          case LONG: nnew = Anum.longValue() - Bnum.longValue(); break;
          case DOUBLE: nnew = Anum.doubleValue() - Bnum.doubleValue(); break;
          default: throw new AssertionError();
        }
        break;
      case DIVIDE:
        switch(scalarType) {
          case LONG: nnew = Anum.longValue() / Bnum.longValue(); break;
          case DOUBLE: nnew = Anum.doubleValue() / Bnum.doubleValue(); break;
          default: throw new AssertionError();
        }
        break;
      case POWER:
        switch(scalarType) {
          case LONG: nnew = (long)Math.pow(Anum.longValue(), Bnum.longValue()); break;
          case DOUBLE: nnew = Math.pow(Anum.doubleValue(), Bnum.doubleValue()); break;
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
      default: throw new AssertionError();
    }
    return vnew;
  }
}
