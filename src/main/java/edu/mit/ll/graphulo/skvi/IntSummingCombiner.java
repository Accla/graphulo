package edu.mit.ll.graphulo.skvi;

import org.apache.accumulo.core.data.Key;

import java.util.Iterator;

/**
 * A Combiner that interprets Values as Longs and returns their sum.
 */
public class IntSummingCombiner extends IntCombiner {
  @Override
  public Integer typedReduce(Key key, Iterator<Integer> iter) {
    int sum = 0;
    while (iter.hasNext()) {
      sum = sum + iter.next();
    }
//    System.out.println("int sum to "+sum+" on key "+key.toStringNoTime());
    return sum;
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("sum");
    io.setDescription("IntegerSummingCombiner interprets Values as Integers and adds them together.");
    return io;
  }
}
