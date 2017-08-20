package edu.mit.ll.graphulo.skvi;

import org.apache.accumulo.core.data.Key;

import java.util.Iterator;

/**
 * A Combiner that interprets Values as Longs and returns their sum.
 */
public class DoubleSummingCombiner extends DoubleCombiner {
  @Override
  public Double typedReduce(Key key, Iterator<Double> iter) {
    double sum = 0;
    while (iter.hasNext()) {
      sum = sum + iter.next();
    }
    return sum;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("sum");
    io.setDescription("DoubleSummingCombiner interprets Values as Doubles and adds them together.");
    return io;
  }
}
