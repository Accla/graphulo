package edu.mit.ll.graphulo.skvi;

import edu.mit.ll.graphulo.DynamicIteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * Solves the problem of running out of iterator priority spaces.
 * Bundles several iterators together in one.
 * @see edu.mit.ll.graphulo.DynamicIteratorSetting
 */
public class DynamicIterator extends WrappingIterator {
  private static final Logger log = LogManager.getLogger(DynamicIterator.class);

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    DynamicIteratorSetting dis = DynamicIteratorSetting.fromMap(options);
    source = dis.loadIteratorStack(source, env);
    setSource(source);
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    DynamicIterator copy = new DynamicIterator();
    copy.setSource(getSource().deepCopy(env));
    return copy;
  }
}
