package edu.mit.ll.graphulo.skvi;

import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

/**
 * Filter based on minimum and maximum Value.
 * Interprets Values as one of the {@link edu.mit.ll.graphulo.simplemult.MathTwoScalar.ScalarType} types,
 * encoded as a String.
 */
public class MinMaxValueFilter extends Filter {
  private static final Logger log = LogManager.getLogger(MinMaxValueFilter.class);

  public static final String MINVALUE = "minValue", MAXVALUE = "maxValue";

  public static IteratorSetting iteratorSetting(int priority, MathTwoScalar.ScalarType numberType, Number min, Number max) {
    IteratorSetting itset = new IteratorSetting(priority, MinMaxValueFilter.class);
    if (numberType != null)
      itset.addOption(MathTwoScalar.SCALAR_TYPE, numberType.name());
    if (min != null)
      itset.addOption(MINVALUE, min.toString());
    if (max != null)
      itset.addOption(MAXVALUE, max.toString());
    return itset;
  }

  private Number minValue = 0l, maxValue = Long.MAX_VALUE;
  private MathTwoScalar.ScalarType scalarType = MathTwoScalar.ScalarType.LONG;

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    if (options.containsKey(MathTwoScalar.SCALAR_TYPE))
      scalarType = MathTwoScalar.ScalarType.valueOf(options.get(MathTwoScalar.SCALAR_TYPE));
    if (options.containsKey(MINVALUE)) {
      switch (scalarType) {
        case LONG: minValue = Long.parseLong(options.get(MINVALUE)); break;
        case DOUBLE: minValue = Double.parseDouble(options.get(MINVALUE)); break;
        case BIGDECIMAL: minValue = new BigDecimal(options.get(MINVALUE)); break;
      }
    } else {
      switch (scalarType) {
        case LONG:        minValue = 0l; break;
        case DOUBLE:      minValue = 0.0d; break;
        case BIGDECIMAL:  minValue = BigDecimal.ZERO; break;
      }
    }
    if (options.containsKey(MAXVALUE)) {
      switch (scalarType) {
        case LONG:        maxValue = Long.parseLong(options.get(    MAXVALUE)); break;
        case DOUBLE:      maxValue = Double.parseDouble(options.get(MAXVALUE)); break;
        case BIGDECIMAL:  maxValue = new BigDecimal(options.get(    MAXVALUE)); break;
      }
    } else {
      switch (scalarType) {
        case LONG:        maxValue = Long.MAX_VALUE; break;
        case DOUBLE:      maxValue = (double)Long.MAX_VALUE; break;
        case BIGDECIMAL:  maxValue = BigDecimal.valueOf(Long.MAX_VALUE); break;
      }
    }
    boolean bad = false;
    switch (scalarType) {
      case LONG:        if (maxValue.longValue() < minValue.longValue()) bad = true; break;
      case DOUBLE:      if (maxValue.longValue() < minValue.longValue()) bad = true; break;
      case BIGDECIMAL:  if (((BigDecimal)maxValue).compareTo((BigDecimal)minValue) < 0) bad = true; break;
    }
    if (bad)
      throw new IllegalArgumentException("maxValue < minValue: "+maxValue+" < "+minValue);
    log.debug("minValue "+minValue+" maxValue"+maxValue);
  }

  @Override
  public boolean accept(Key k, Value v) {
    switch (scalarType) {
      case LONG:
        long l = Long.parseLong(new String(v.get(), StandardCharsets.UTF_8));
//    if (l >= minValue.longValue() && l <= maxValue.longValue()) // DEBUG
//      log.info("accept: "+k+" -> "+v);
//    else {
//      log.info("REJECT: "+k+" -> "+v);
//    }
        return l >= minValue.longValue() && l <= maxValue.longValue();
      case DOUBLE:
        double d = Double.parseDouble(new String(v.get(), StandardCharsets.UTF_8));
//        if (d >= minValue.doubleValue() && d <= maxValue.doubleValue()) // DEBUG
//          log.info("accept: "+k.toStringNoTime()+" -> "+v);
//        else {
//          log.info("REJECT: "+k.toStringNoTime()+" -> "+v);
//        }
        return d >= minValue.doubleValue() && d <= maxValue.doubleValue();
      case BIGDECIMAL:
        BigDecimal b = new BigDecimal(new String(v.get(), StandardCharsets.UTF_8));
        return b.compareTo((BigDecimal)minValue) >= 0 && b.compareTo((BigDecimal)maxValue) <= 0;
      default:
        throw new UnsupportedOperationException("ScalarType not supported: "+scalarType);
    }
  }

  @Override
  public MinMaxValueFilter deepCopy(IteratorEnvironment env) {
    MinMaxValueFilter copy = (MinMaxValueFilter)super.deepCopy(env);
    copy.scalarType = scalarType;
    copy.minValue = minValue;
    copy.maxValue = maxValue;
    return copy;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName(MinMaxValueFilter.class.getCanonicalName());
    io.setDescription("Filter based on Value interpreted as a Long, encoded as String");
    io.addNamedOption(MathTwoScalar.SCALAR_TYPE, "How to interpret Value encoded as String: " + Arrays.toString(MathTwoScalar.ScalarType.values()));
    io.addNamedOption(MINVALUE, "Minimum Value, default "+minValue);
    io.addNamedOption(MAXVALUE, "Maximum Value, default "+maxValue);
    return io;
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    Number minValue = 0l, maxValue = Long.MAX_VALUE;
    MathTwoScalar.ScalarType scalarType = MathTwoScalar.ScalarType.LONG;
    if (options.containsKey(MathTwoScalar.SCALAR_TYPE))
      scalarType = MathTwoScalar.ScalarType.valueOf(options.get(MathTwoScalar.SCALAR_TYPE));
    if (options.containsKey(MINVALUE)) {
      switch (scalarType) {
        case LONG: minValue = Long.parseLong(options.get(MINVALUE)); break;
        case DOUBLE: minValue = Double.parseDouble(options.get(MINVALUE)); break;
        case BIGDECIMAL: minValue = new BigDecimal(options.get(MINVALUE)); break;
      }
    } else {
      switch (scalarType) {
        case LONG:        minValue = 0l; break;
        case DOUBLE:      minValue = 0.0d; break;
        case BIGDECIMAL:  minValue = BigDecimal.ZERO; break;
      }
    }
    if (options.containsKey(MAXVALUE)) {
      switch (scalarType) {
        case LONG:        maxValue = Long.parseLong(options.get(    MAXVALUE)); break;
        case DOUBLE:      maxValue = Double.parseDouble(options.get(MAXVALUE)); break;
        case BIGDECIMAL:  maxValue = new BigDecimal(options.get(    MAXVALUE)); break;
      }
    } else {
      switch (scalarType) {
        case LONG:        maxValue = Long.MAX_VALUE; break;
        case DOUBLE:      maxValue = (double)Long.MAX_VALUE; break;
        case BIGDECIMAL:  maxValue = BigDecimal.valueOf(Long.MAX_VALUE); break;
      }
    }
    boolean bad = false;
    switch (scalarType) {
      case LONG:        if (maxValue.longValue() < minValue.longValue()) bad = true; break;
      case DOUBLE:      if (maxValue.longValue() < minValue.longValue()) bad = true; break;
      case BIGDECIMAL:  if (((BigDecimal)maxValue).compareTo((BigDecimal)minValue) < 0) bad = true; break;
    }
    if (bad)
      throw new IllegalArgumentException("maxValue < minValue: "+maxValue+" < "+minValue);
    return super.validateOptions(options);
  }
}
