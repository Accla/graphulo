/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
