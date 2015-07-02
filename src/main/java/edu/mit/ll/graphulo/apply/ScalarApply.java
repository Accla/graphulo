package edu.mit.ll.graphulo.apply;

import com.google.common.collect.Iterators;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Set equal to, add, subtract, multiply or divide by a number.
 */
public class ScalarApply implements ApplyOp {
  private static final Logger log = LogManager.getLogger(ScalarApply.class);

  public enum ScalarOp {
    PLUS, TIMES, SET, MINUS_SCALAR_RIGHT, MINUS_SCALAR_LEFT,
    DIVIDE_SCALAR_RIGHT, DIVIDE_SCALAR_LEFT
  }
  public enum ScalarType {
    LONG, DOUBLE
  }

  public static final String SCALAR = "scalar",
      SCALAR_OP = "ScalarOp",
      SCALAR_TYPE = "scalarType";

  /** Create an IteratorSetting that performs a ScalarOp on every Value it sees, parsing Values as Doubles. */
  public static IteratorSetting iteratorSettingDouble(int priority, ScalarOp op, double scalar) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, ScalarApply.class.getName());
    itset.addOption(ApplyIterator.APPLYOP+ApplyIterator.OPT_SUFFIX+SCALAR_OP, op.name());
    itset.addOption(ApplyIterator.APPLYOP+ApplyIterator.OPT_SUFFIX+SCALAR_TYPE, ScalarType.DOUBLE.name());
    itset.addOption(ApplyIterator.APPLYOP+ApplyIterator.OPT_SUFFIX+SCALAR, Double.toString(scalar));
    return itset;
  }

  /** Create an IteratorSetting that performs a ScalarOp on every Value it sees, parsing Values as Longs. */
  public static IteratorSetting iteratorSettingLong(int priority, ScalarOp op, long scalar) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, ScalarApply.class.getName());
    itset.addOption(ApplyIterator.APPLYOP+ApplyIterator.OPT_SUFFIX+SCALAR_OP, op.name());
    itset.addOption(ApplyIterator.APPLYOP+ApplyIterator.OPT_SUFFIX+SCALAR_TYPE, ScalarType.LONG.name());
    itset.addOption(ApplyIterator.APPLYOP+ApplyIterator.OPT_SUFFIX+SCALAR, Long.toString(scalar));
    return itset;
  }

//  /** Shortcut method for common Abs0 op */
//  public static IteratorSetting abs0Apply(int priority) {
//    return iteratorSettingLong(priority, 1, ScalarOp.SET);
//  }

  private Number scalar;
  private ScalarType scalarType = ScalarType.DOUBLE;
  private ScalarOp scalarOp;

  private void parseOptions(Map<String,String> options) {
    if (options.containsKey(SCALAR_TYPE))
      scalarType = ScalarType.valueOf(options.get(SCALAR_TYPE));

    for (Map.Entry<String, String> entry : options.entrySet()) {
      String v = entry.getValue();
      switch (entry.getKey()) {
        case SCALAR:
          switch(scalarType) {
            case LONG: scalar = Long.valueOf(v); break;
            case DOUBLE: scalar = Double.valueOf(v); break;
          } break;
        case SCALAR_OP: scalarOp = ScalarOp.valueOf(v); break;
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
  public Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v) {
    if (scalarOp == ScalarOp.SET) {
      Value vnew;
      switch(scalarType) {
        case LONG: vnew = new Value(Long.toString(scalar.longValue()).getBytes()); break;
        case DOUBLE: vnew = new Value(Double.toString(scalar.doubleValue()).getBytes()); break;
        default: throw new AssertionError();
      }
      return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, vnew));
    }

    Number vnum;
    switch(scalarType) {
      case LONG: vnum = Long.valueOf(new String(v.get())); break;
      case DOUBLE: vnum = Double.valueOf(new String(v.get())); break;
      default: throw new AssertionError();
    }
    Number nnew;
    switch(scalarOp) {
      case PLUS:
        switch(scalarType) {
          case LONG: nnew = vnum.longValue() + scalar.longValue(); break;
          case DOUBLE: nnew = vnum.doubleValue() + scalar.doubleValue(); break;
          default: throw new AssertionError();
        }
        break;
      case TIMES:
        switch(scalarType) {
          case LONG: nnew = vnum.longValue() * scalar.longValue(); break;
          case DOUBLE: nnew = vnum.doubleValue() * scalar.doubleValue(); break;
          default: throw new AssertionError();
        }
        break;
      case MINUS_SCALAR_LEFT:
        switch(scalarType) {
          case LONG: nnew = scalar.longValue() - vnum.longValue(); break;
          case DOUBLE: nnew = scalar.doubleValue() - vnum.doubleValue(); break;
          default: throw new AssertionError();
        }
        break;
      case MINUS_SCALAR_RIGHT:
        switch(scalarType) {
          case LONG: nnew = vnum.longValue() - scalar.longValue(); break;
          case DOUBLE: nnew = vnum.doubleValue() - scalar.doubleValue(); break;
          default: throw new AssertionError();
        }
        break;
      case DIVIDE_SCALAR_RIGHT:
        switch(scalarType) {
          case LONG: nnew = vnum.longValue() / scalar.longValue(); break;
          case DOUBLE: nnew = vnum.doubleValue() / scalar.doubleValue(); break;
          default: throw new AssertionError();
        }
        break;
      case DIVIDE_SCALAR_LEFT:
        switch(scalarType) {
          case LONG: nnew = scalar.longValue() / vnum.longValue(); break;
          case DOUBLE: nnew = scalar.doubleValue() / vnum.doubleValue(); break;
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
    return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, vnew));
  }
}
