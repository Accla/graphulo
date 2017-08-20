package edu.mit.ll.graphulo.skvi;

import org.apache.accumulo.core.data.Key;

import java.util.Iterator;

/**
 * A Combiner that interprets Values as Longs and returns their sum.
 */
public class OddDivideIntSummingCombiner extends IntCombiner {
  @Override
  public Integer typedReduce(Key key, Iterator<Integer> iter) {
    int sum = 0;
    while (iter.hasNext()) {
      final int i = iter.next();
      if( i % 2 == 0 )
        continue;
      sum = sum + (i-1)/2;
    }
    return sum;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("sum");
    io.setDescription("OddDivideIntSummingCombiner interprets Values as Integers, filters out even values (keeping odd values) and adds them as (value-1)/2.");
    return io;
  }
}
