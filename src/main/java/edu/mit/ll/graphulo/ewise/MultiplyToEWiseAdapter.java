package edu.mit.ll.graphulo.ewise;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.rowmult.MultiplyOp;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A EWiseOp, which must have the same colF and colQ,
 * can use the implementation of a MultiplyOp, which can have diferent colF and colQ,
 * by passing the same colF and colQ to the MultiplyOp.
 * <p>
 *  The {@value #SWITCHARGS} option switches the position of the A and B values.
 *  All options that are not {@value #MULTIPLYOP} or {@value #SWITCHARGS}
 *  are passed to the MultiplyOp.
 *  @deprecated This is a bad idea. MultiplyOp handles keys differently from EWiseOp.
 *    Maybe this class could still have some use for ops that do not affect the Key as normal.
 */
@Deprecated
public class MultiplyToEWiseAdapter implements EWiseOp {

  public static final String MULTIPLYOP ="multiplyOpForEWise", SWITCHARGS = "multiplyOpSwitchArgsForEWise";

  private MultiplyOp multiplyOp;
  private boolean switchArgs = false;


  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    String clazz = options.get(MULTIPLYOP);
    Preconditions.checkArgument(clazz != null, "Required option %s. Given: %s", MULTIPLYOP, options);
    multiplyOp = GraphuloUtil.subclassNewInstance(clazz, MultiplyOp.class);

    if (options.containsKey(SWITCHARGS))
      switchArgs = Boolean.parseBoolean(options.get(SWITCHARGS));

    Map<String,String> otherOpts = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String k = entry.getKey();
      if (!k.equals(MULTIPLYOP) && !k.equals(SWITCHARGS))
        otherOpts.put(entry.getKey(), entry.getValue());
    }
    multiplyOp.init(otherOpts, env);
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> multiply(
      ByteSequence Mrow, ByteSequence McolF, ByteSequence McolQ, ByteSequence McolVis,
      long Atime, long Btime,
      Value Aval, Value Bval) {
    if (switchArgs)
      return multiplyOp.multiply(Mrow, McolF, McolQ, null, Btime, McolF, McolQ, null, Atime, Bval, Aval);
    else
      return multiplyOp.multiply(Mrow, McolF, McolQ, null, Atime, McolF, McolQ, null, Btime, Aval, Bval);
  }
}
