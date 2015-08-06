package edu.mit.ll.graphulo.apply;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.skvi.RemoteSourceIterator;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
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
 * <p>
 * Preserves keys.
 */
public class JaccardDegreeApply implements ApplyOp {
  private static final Logger log = LogManager.getLogger(JaccardDegreeApply.class);

  /** Setup with {@link edu.mit.ll.graphulo.Graphulo#basicRemoteOpts(String, String, String, Authorizations)}
   * options for RemoteSourceIterator. */
  public static IteratorSetting iteratorSetting(int priority, Map<String,String> remoteOpts) {
    IteratorSetting JDegApply = new IteratorSetting(priority, ApplyIterator.class, remoteOpts);
    JDegApply.addOption(ApplyIterator.APPLYOP, JaccardDegreeApply.class.getName());
    return JDegApply;
  }

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
          Double.valueOf(remoteDegTable.getTopValue().toString()));
      remoteDegTable.next();
    }
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(final Key k, Value v) {
//    Key newKey = ColQSpecialByteApply.removeSpecialBytes(k);
//    if (newKey == null)
//      return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, v));
    // Period indicates already processed Double value. No period indicates unprocessed Long value.
    String vstr = v.toString();
    if (vstr.contains("."))
      return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, v));

    String row = k.getRow().toString(), col = k.getColumnQualifier().toString();
    Double rowDeg = degMap.get(row), colDeg = degMap.get(col);
    if (rowDeg == null)
      throw new IllegalStateException("Cannot find rowDeg in degree table:" +row);
    if (colDeg == null)
      throw new IllegalStateException("Cannot find colDeg in degree table:" +col);
    double Jij = Long.parseLong(vstr);
    return Iterators.singletonIterator( new AbstractMap.SimpleImmutableEntry<>(k,
        new Value(Double.toString(Jij / (rowDeg+colDeg-Jij)).getBytes())
    ));
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
  }
}
