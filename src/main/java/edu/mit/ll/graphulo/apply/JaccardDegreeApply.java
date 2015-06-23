package edu.mit.ll.graphulo.apply;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.skvi.RemoteSourceIterator;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Applies <tt>J_ij = J_ij / (d_i + d_j - J_ij)</tt>.
 * <p>
 *   Only acts when the column qualifier ends with two 0 bytes. This gives idempotency at the expense of a "hack."
 * <p>
 * Possible future optimization: only need to scan the part of the degree table
 * that is after the seek range's beginning row. For example, if seeked to [v3,v5),
 * we should scan the degree table on (v3,+inf) and load those degrees into a Map.
 */
public class JaccardDegreeApply implements ApplyOp {
  private static final Logger log = LogManager.getLogger(ApplyIterator.class);

  private RemoteSourceIterator remoteDegTable;
  private Map<String,Double> degMap;

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    remoteDegTable = new RemoteSourceIterator();
    remoteDegTable.init(null, options, env);
    degMap = new HashMap<>();
    scanDegreeTable();
  }

  private void scanDegreeTable() throws IOException {
    remoteDegTable.seek(new Range(), Collections.<ByteSequence>emptySet(), false);
    Text rowHolder = new Text();
    while (remoteDegTable.hasTop()) {
      degMap.put(remoteDegTable.getTopKey().getRow(rowHolder).toString(),
          (double) Long.parseLong(remoteDegTable.getTopValue().toString()));
      remoteDegTable.next();
    }
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(final Key k, Value v) {
    Key newKey = ColQSpecialByteApply.removeSpecialBytes(k);
    if (newKey == null)
      return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, v));

    String row = k.getRow().toString(), col = newKey.getColumnQualifier().toString();
    Double rowDeg = degMap.get(row), colDeg = degMap.get(col);
    if (rowDeg == null)
      throw new IllegalStateException("Cannot find rowDeg in degree table:" +row);
    if (colDeg == null)
      throw new IllegalStateException("Cannot find colDeg in degree table:" +col);
//    double rowDeg = degMap.get(row), colDeg = degMap.get(col),
    double Jij = Long.parseLong(v.toString());
    return Iterators.singletonIterator( new AbstractMap.SimpleImmutableEntry<>(newKey,
        new Value(Double.toString(Jij / (rowDeg+colDeg-Jij)).getBytes())
    ));
  }
}
