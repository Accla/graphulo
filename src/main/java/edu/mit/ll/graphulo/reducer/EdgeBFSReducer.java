package edu.mit.ll.graphulo.reducer;

import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

/**
 * Column "in|v3" ==> "v3".
 * Stores a set of the columns reached in one step of BFS on the incidence matrix.
 */
public class EdgeBFSReducer implements Reducer<HashSet<String>> {
  private static final Logger log = LogManager.getLogger(EdgeBFSReducer.class);

  public static final String IN_COLUMN_PREFIX = "inColumnPrefix";
  private byte[] inColumnPrefix;

  private HashSet<String> setNodesReached = new HashSet<>();

  private void parseOptions(Map<String, String> options) {
    for (Map.Entry<String, String> optionEntry : options.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
      switch (optionKey) {
        case IN_COLUMN_PREFIX:
          inColumnPrefix = optionValue.getBytes();
          break;
        default:
          log.warn("Unrecognized option: " + optionEntry);
          continue;
      }
    }
    if (inColumnPrefix == null)
      throw new IllegalArgumentException("no "+IN_COLUMN_PREFIX);
  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) {
      parseOptions(options);
  }

  @Override
  public void reset() throws IOException {
    setNodesReached.clear();
  }

  @Override
  public void update(Key k, Value v) {
    String nodeAfter = GraphuloUtil.stringAfter(inColumnPrefix, k.getColumnQualifier().getBytes());
    if (nodeAfter != null)
      setNodesReached.add(nodeAfter);
//    log.debug("received colQ "+k.getColumnQualifier().toString()+" : now "+setNodesReached.toString());
  }

  @Override
  public void combine(HashSet<String> another) {
    setNodesReached.addAll(another);
  }

  @Override
  public boolean hasTop() {
    return !setNodesReached.isEmpty();
  }

  @Override
  public HashSet<String> getForClient() {
    return setNodesReached;
  }
}
