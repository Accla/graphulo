package edu.mit.ll.graphulo.reducer;

import edu.mit.ll.graphulo.Reducer;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

/**
 * Row "v1|v2" ==> "v2".
 * Stores a set of the "in-node" part of rows reached in one step of BFS on a single-table schema.
 */
public class SingleBFSReducer implements Reducer<HashSet<String>> {
  private static final Logger log = LogManager.getLogger(SingleBFSReducer.class);

  public static final String EDGE_SEP = "edgeSep";
  private char edgeSep;

  private HashSet<String> setNodesReached = new HashSet<>();

  private void parseOptions(Map<String, String> options) {
    boolean gotFieldSep = false;
    for (Map.Entry<String, String> optionEntry : options.entrySet()) {
      String optionKey = optionEntry.getKey();
      String optionValue = optionEntry.getValue();
      switch (optionKey) {
        case EDGE_SEP:
          if (optionValue.length() != 1)
            throw new IllegalArgumentException("bad "+ EDGE_SEP +": "+optionValue);
          edgeSep = optionValue.charAt(0);
          gotFieldSep = true;
          break;
        default:
          log.warn("Unrecognized option: " + optionEntry);
          continue;
      }
    }
    if (!gotFieldSep)
      throw new IllegalArgumentException("no "+ EDGE_SEP);
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
    String rStr;
    {
      ByteSequence rowData = k.getRowData();
      rStr = new String(rowData.getBackingArray(), rowData.offset(), rowData.length());
    }
    int pos = rStr.indexOf(edgeSep);
    if (pos == -1)
      return;        // this is a degree row, not an edge row.
    String toNode = rStr.substring(pos+1);
    setNodesReached.add(toNode);
//    log.debug("edge row "+rStr+" : now "+setNodesReached.toString());
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
  public HashSet<String> get() {
    return setNodesReached;
  }
}
