package edu.mit.ll.graphulo.tricount;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.WritableComparator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Like {@link edu.mit.ll.graphulo.skvi.TriangularFilter}, except that it skips the first byte of the row if the row has length 5.
 */
public class TriangularFilter_TriCountMagic extends Filter {

  public enum TriangularType { Upper, UpperDiagonal, Lower, LowerDiagonal, Diagonal, NoDiagonal }
  public static final String TRIANGULAR_TYPE = "triangularType";

  public static IteratorSetting iteratorSetting(int priority, TriangularType type) {
    IteratorSetting itset = new IteratorSetting(priority, TriangularFilter_TriCountMagic.class);
    itset.addOption(TRIANGULAR_TYPE, type.name());
    return itset;
  }

  private TriangularType triangularType = TriangularType.Upper;

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env); // initializes NEGATE
    if (options.containsKey(TRIANGULAR_TYPE))
      triangularType = TriangularType.valueOf(options.get(TRIANGULAR_TYPE));
  }

  @Override
  public boolean accept(Key k, Value v) {
    final ByteSequence ro = k.getRowData();
    final ByteSequence cq = k.getColumnQualifierData();
    final int cmp;
//    if( ro.length() == 5 ) {
      if (ro.isBackedByArray() && cq.isBackedByArray())
        cmp = WritableComparator.compareBytes(ro.getBackingArray(), ro.offset() + 1, ro.length() - 1, cq.getBackingArray(), cq.offset(), cq.length());
      else
        cmp = compareBytesMagic(ro, cq);
//    }
//    else
//      cmp = ro.compareTo(cq);
//    System.out.println(Arrays.toString(ro.toArray())+" x "+Arrays.toString(cq.toArray())+" ==> "+cmp);
    switch (triangularType) {
      case Upper: return cmp < 0;
      case UpperDiagonal: return cmp <= 0;
      case Lower: return cmp > 0;
      case LowerDiagonal: return cmp >= 0;
      case Diagonal: return cmp == 0;
      case NoDiagonal: return cmp != 0;
      default: throw new AssertionError();
    }
  }

  public static int compareBytesMagic(ByteSequence bs1, ByteSequence bs2) {
    int minLen = Math.min(bs1.length()-1, bs2.length());
    for (int i = 0; i < minLen; i++) {
      int a = (bs1.byteAt(i+1) & 0xff);
      int b = (bs2.byteAt(i) & 0xff);
      if (a != b)
        return a - b;
    }
    return bs1.length() - bs2.length();
  }

  @Override
  public TriangularFilter_TriCountMagic deepCopy(IteratorEnvironment env) {
    TriangularFilter_TriCountMagic copy = (TriangularFilter_TriCountMagic)super.deepCopy(env);
    copy.triangularType = triangularType;
    return copy;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName(TriangularFilter_TriCountMagic.class.getCanonicalName());
    io.setDescription("Filter based on relative ordering of row and column qualifier, skipping first byte of row if the row has length 5");
    io.addNamedOption(TRIANGULAR_TYPE, "TriangularFilter type: one of "+Arrays.toString(TriangularType.values())+", default "+triangularType);
    return io;
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    if (options.containsKey(TRIANGULAR_TYPE)) {
      TriangularType.valueOf(options.get(TRIANGULAR_TYPE));
    }
    return super.validateOptions(options);
  }


}
