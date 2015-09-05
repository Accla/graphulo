package edu.mit.ll.graphulo.skvi;

import com.google.common.collect.ImmutableSortedMap;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.SortedMap;

/**
 * For testing; an iterator that emits entries from a list of hardcoded data.
 */
public class HardListIterator extends MapIterator {
  public final static SortedMap<Key, Value> allEntriesToInject =
      ImmutableSortedMap.<Key, Value>naturalOrder()
          .put(new Key(new Text("a1"), new Text("colF3"), new Text("colQ3"), System.currentTimeMillis()),
              new Value("1".getBytes()))
          .put(new Key(new Text("c1"), new Text("colF3"), new Text("colQ3"), System.currentTimeMillis()),
              new Value("1".getBytes()))
          .put(new Key(new Text("m1"), new Text("colF3"), new Text("colQ3"), System.currentTimeMillis()),
              new Value("1".getBytes()))
          .build();

  public HardListIterator() {
    super(allEntriesToInject);
  }
}
