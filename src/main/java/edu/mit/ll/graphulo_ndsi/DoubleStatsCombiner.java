package edu.mit.ll.graphulo_ndsi;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Like {@link org.apache.accumulo.examples.simple.combiner.StatsCombiner}
 * except acts on double Values instead of doubles.
 * Format is "min,max,sum,count".
 */
public class DoubleStatsCombiner extends Combiner {

  @Override
  public Value reduce(Key key, Iterator<Value> iter) {

    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;
    double sum = 0;
    long count = 0;

    while (iter.hasNext()) {
      String stats[] = iter.next().toString().split(",");

      if (stats.length == 1) {
        double val = Double.parseDouble(stats[0]);
        min = Math.min(val, min);
        max = Math.max(val, max);
        sum += val;
        count += 1;
      } else {
        min = Math.min(Double.parseDouble(stats[0]), min);
        max = Math.max(Double.parseDouble(stats[1]), max);
        sum += Double.parseDouble(stats[2]);
        count += Long.parseLong(stats[3]);
      }
    }

    String ret = Double.toString(min) + "," + Double.toString(max) + "," + Double.toString(sum) + "," + Long.toString(count);
    return new Value(ret.getBytes());
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);

  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("doubleStatsCombiner");
    io.setDescription("Combiner that keeps track of min, max, sum, and count");
    return io;
  }

  public static IteratorSetting iteratorSetting(int priority, List<IteratorSetting.Column> columns) {
    IteratorSetting itset = new IteratorSetting(priority, DoubleStatsCombiner.class);
    if (columns == null || columns.isEmpty())
      Combiner.setCombineAllColumns(itset, true);
    else
      Combiner.setColumns(itset, columns);
    return itset;
  }

}