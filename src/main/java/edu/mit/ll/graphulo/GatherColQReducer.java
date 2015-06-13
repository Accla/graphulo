package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

/**
 * Gathers column qualifiers into a set.
 */
public class GatherColQReducer implements Reducer<HashSet<String>> {
  private HashSet<String> setColQ = new HashSet<>();
  private Text tmpTextColQ = new Text();

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env)  {

  }

  @Override
  public void reset() throws IOException {
    setColQ.clear();
  }

  @Override
  public void update(Key k, Value v) {
    setColQ.add(k.getColumnQualifier(tmpTextColQ).toString());
  }

  @Override
  public void combine(HashSet<String> another) {
    setColQ.addAll(another);
  }

  @Override
  public boolean hasTop() {
    return !setColQ.isEmpty();
  }


  @Override
  public HashSet<String> get() {
    return setColQ;
  }

}
