package edu.mit.ll.graphulo_ocean;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.apply.ApplyOp;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import static edu.mit.ll.graphulo.skvi.D4mRangeFilter.KeyPart;

/**
 * Reverse Complement the bases in the given KeyPart. Default is ROW.
 * Unless the KeyPart is Value, this class changes the entries' sort order.
 */
public class ReverseComplementApply implements ApplyOp {

  public static final String KEYPART = "keyPart", OPT_K = "K";

  public static IteratorSetting iteratorSetting(int priority, KeyPart keyPart, int K) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, ReverseComplementApply.class.getName());
    if (keyPart != null)
      itset.addOption(ApplyIterator.APPLYOP+ GraphuloUtil.OPT_SUFFIX+KEYPART, keyPart.name());
    itset.addOption(ApplyIterator.APPLYOP+ GraphuloUtil.OPT_SUFFIX+OPT_K, Integer.toString(K));
    return itset;
  }

  private KeyPart keyPart = KeyPart.ROW;
  private GenomicEncoder G = null;

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    if (options.containsKey(OPT_K))
      G = new GenomicEncoder(Integer.parseInt(options.get(OPT_K)));
    if (options.containsKey(KEYPART))
      keyPart = KeyPart.valueOf(options.get(KEYPART));
  }

//  private byte[] getKeyBytes(Key k, Value v) {
//    switch (keyPart) {
//      case ROW:
//        return k.getRowData().toArray();
//      case COLF:
//        return k.getColumnFamilyData().toArray();
//      case COLQ:
//        return k.getColumnQualifierData().toArray();
//      case VAL:
//        return v.get();
//      default: throw new AssertionError();
//    }
//  }


  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v) throws IOException {
    byte[] seqBytes;// = getKeyBytes(k, v);
//    boolean copy = false;
    switch (keyPart) {
      case ROW:
//        ByteSequence data = k.getRowData();
//        if (data.offset() != 0 && data.length() == G.K) {
//
//        }
        // update the underlying bytes directly
        seqBytes = k.getRowData().getBackingArray(); // not using toArray
        break;
      case COLF:
        seqBytes = k.getColumnFamilyData().getBackingArray();
        break;
      case COLQ:
        seqBytes = k.getColumnQualifierData().getBackingArray();
        break;
      case VAL:
        seqBytes = v.get();
        break;
      default: throw new AssertionError();
    }
    if (G == null)
      G = new GenomicEncoder(seqBytes.length);
    G.reverseComplement(seqBytes);
    return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, v));
  }


  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {

  }
}
