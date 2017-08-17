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

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.util.PeekingIterator1;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import javax.annotation.Nullable;

/**
 * A Combiner that emits any number of entries as a combination of the entries it sees
 * with the same row, column family, and column qualifier.
 *
 * @see org.apache.accumulo.core.iterators.Combiner
 */
public abstract class MultiCombiner extends MultiKeyCombiner {
  @Override
  public Iterator<? extends Map.Entry<Key, Value>> reduceKV(Iterator<Map.Entry<Key, Value>> iter) {
    if (!iter.hasNext())
      return null;
    Map.Entry<Key, Value> first = iter.next();
    final Key firstKey = first.getKey(); // no defensive copy necesary due to superclass
    return Iterators.transform(reduce(firstKey,
        new PeekingIterator1<>(
            Iterators.transform(iter,
                new Function<Map.Entry<Key, Value>, Value>() {
                  @Nullable
                  @Override
                  public Value apply(@Nullable Map.Entry<Key, Value> input) {
                    return input == null ? null : input.getValue();
                  }
                }), first.getValue())),
        new Function<Value, Map.Entry<Key, Value>>() {
          @Nullable
          @Override
          public Map.Entry<Key, Value> apply(@Nullable Value input) {
            return new AbstractMap.SimpleImmutableEntry<>(firstKey, input);
          }
        });
  }

  public abstract Iterator<Value> reduce(Key key, Iterator<Value> iter);
}
