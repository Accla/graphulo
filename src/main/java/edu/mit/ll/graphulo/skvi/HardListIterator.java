package edu.mit.ll.graphulo.skvi;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * For testing; an iterator that emits entries from a list of hardcoded data.
 */
public class HardListIterator extends MapIterator {
  public final static SortedMap<Key, Value> allEntriesToInject;

  static {
    SortedMap<Key, Value> t = new TreeMap<>();
    t.put(new Key(new Text("a1"), new Text("colF3"), new Text("colQ3"), System.currentTimeMillis()),
        new Value("1".getBytes()));
    t.put(new Key(new Text("c1"), new Text("colF3"), new Text("colQ3"), System.currentTimeMillis()),
        new Value("1".getBytes()));
    t.put(new Key(new Text("m1"), new Text("colF3"), new Text("colQ3"), System.currentTimeMillis()),
        new Value("1".getBytes()));
    allEntriesToInject = Collections.unmodifiableSortedMap(t); // for safety
  }

  public HardListIterator() {
    super(allEntriesToInject);
  }
}
