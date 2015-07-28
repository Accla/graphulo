package edu.mit.ll.graphulo.apply;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * For every entry passed in, emits <tt>knum</tt> entries,
 * each with the same row and with column qualifiers 1, 2, 3, ..., knum.
 * Values are random doubles between 0 and 1.
 */
public class RandomTopicApply implements ApplyOp {
  private static final Logger log = LogManager.getLogger(RandomTopicApply.class);

  public static final String KNUM = "knum";

  public static IteratorSetting iteratorSetting(int priority, int knum) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, RandomTopicApply.class.getName());
    itset.addOption(ApplyIterator.APPLYOP+ApplyIterator.OPT_SUFFIX+KNUM, Integer.toString(knum));
    return itset;
  }

  private int knum;

  private void parseOptions(Map<String,String> options) {
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String v = entry.getValue();
      // can replace with switch in Java 1.7
      String s = entry.getKey();
      if (s.equals(KNUM)) {
        knum = Integer.parseInt(v);

      } else {
        log.warn("Unrecognized option: " + entry);

      }
    }
    if (knum <= 0)
      throw new IllegalArgumentException("Bad knum: "+knum);
  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    parseOptions(options);
  }

  private static final Text EMPTY_TEXT = new Text();
  private static final Random rand = new Random();

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v) {
    Text row = k.getRow();
    SortedMap<Key,Value> map = new TreeMap<Key, Value>();
    for (int i = 1; i <= knum; i++) {
      Key knew = new Key(row, EMPTY_TEXT, new Text(Integer.toString(i)), System.currentTimeMillis());

      Value vnew = new Value(Double.toString(
          Math.abs(rand.nextGaussian()))        // absolute value of random normal
          .getBytes());
      map.put(knew, vnew);
    }
    return map.entrySet().iterator();
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {

  }

}
