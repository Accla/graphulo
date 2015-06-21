package edu.mit.ll.graphulo.skvi;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;

import java.io.IOException;
import java.util.Map;

/**
 * Filter based on minimum and maximum long Value.
 * Could generalize to use different encoders. Uses the String encoder presently.
 */
public class MinMaxValueFilter extends Filter {

  public static final String MINVALUE = "minValue", MAXVALUE = "maxValue";

  private long minValue = 0, maxValue = Integer.MAX_VALUE;

  private static final TypedValueCombiner.Encoder<Long> encoder = new LongCombiner.StringEncoder();

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (options.containsKey(MINVALUE)) {
      minValue = Integer.parseInt(options.get(MINVALUE));
    }
    if (options.containsKey(MINVALUE)) {
      maxValue = Integer.parseInt(options.get(MAXVALUE));
    }
    if (maxValue < minValue)
      throw new IllegalArgumentException("maxValue < minValue: "+maxValue+" < "+minValue);
  }

  @Override
  public boolean accept(Key k, Value v) {
    long l = encoder.decode(v.get());
    return l >= minValue && l <= maxValue;
  }

  @Override
  public MinMaxValueFilter deepCopy(IteratorEnvironment env) {
    MinMaxValueFilter copy = (MinMaxValueFilter)super.deepCopy(env);
    copy.minValue = minValue;
    copy.maxValue = maxValue;
    return copy;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName(MinMaxValueFilter.class.getCanonicalName());
    io.setDescription("Filter based on Value interpreted as a Long, encoded as String");
    io.addNamedOption(MINVALUE, "Minimum Value, default "+minValue);
    io.addNamedOption(MINVALUE, "Maximum Value, default "+maxValue);
    return io;
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    int minValue, maxValue;
    if (options.containsKey(MINVALUE)) {
      try {
        minValue = Integer.parseInt(options.get(MINVALUE));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("bad integer " + MINVALUE + ":" + options.get(MINVALUE));
      }
      try {
        maxValue = Integer.parseInt(options.get(MAXVALUE));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("bad integer " + MAXVALUE + ":" + options.get(MAXVALUE));
      }
      if (maxValue < minValue)
        throw new IllegalArgumentException("maxValue < minValue: "+maxValue+" < "+minValue);
    }
    return super.validateOptions(options);
  }
}
