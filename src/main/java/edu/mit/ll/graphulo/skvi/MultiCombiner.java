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
