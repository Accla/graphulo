package edu.mit.ll.graphulo.skvi;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Emits only entries in the XXXm triangle of the table,
 * where XXX is Upper, UpperDiagonal, Lower, LowerDiagonal, Diagonal, or NoDiagonal.
 * Filters based on relative ordering of row and column qualifier.
 *
 * The negation of Upper is LowerDiagonal,
 * of UpperDiagonal is Lower,
 * of Diagonal is NoDiagonal.
 */
public class TriangularFilter extends Filter {

  public enum TriangularType { Upper, UpperDiagonal, Lower, LowerDiagonal, Diagonal, NoDiagonal }
  public static final String TRIANGULAR_TYPE = "triangularType";

  public static IteratorSetting iteratorSetting(int priority, TriangularType type) {
    IteratorSetting itset = new IteratorSetting(priority, TriangularFilter.class);
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
    int cmp = k.getRowData().compareTo(k.getColumnQualifierData());
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

  @Override
  public TriangularFilter deepCopy(IteratorEnvironment env) {
    TriangularFilter copy = (TriangularFilter)super.deepCopy(env);
    copy.triangularType = triangularType;
    return copy;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName(TriangularFilter.class.getCanonicalName());
    io.setDescription("Filter based on relative ordering of row and column qualifier");
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
