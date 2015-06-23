package edu.mit.ll.graphulo.skvi;

import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * SpGEMM on Accumulo tables: C += A * B. Requires transpose(A)=AT is stored as a separate table.
 * Run on table B and pass as an option the connection information to table AT. Or vice versa.
 * Reads from remote table iterator and from local iterator, performs outer product,
 *   sends partial products to result table C.
 * If C is not given, then emits the partial products as part of the query instead of using a BatchWriter.
 */
public class TableMultIterator extends BranchIterator {
  private static final Logger log = LogManager.getLogger(TableMultIterator.class);
  public static final String PREFIX_C = "C";


  @Override
  public SortedKeyValueIterator<Key, Value> initBranchAfterIterator(final SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    // parse options
    Map<String, String> optDM = new HashMap<>(), optC = new HashMap<>();
    {
      Map<String, Map<String, String>> prefixMap = GraphuloUtil.splitMapPrefix(options);
      for (Map.Entry<String, Map<String, String>> prefixEntry : prefixMap.entrySet()) {
        final String prefix = prefixEntry.getKey();
        Map<String, String> entryMap = prefixEntry.getValue();

        switch (prefix) {
          case TwoTableIterator.PREFIX_AT:
          case TwoTableIterator.PREFIX_B:
            optDM.putAll(GraphuloUtil.preprendPrefixToKey(prefix + '.', entryMap));
            break;
          case PREFIX_C:
            optC.putAll(entryMap);
            break;
          default:
            for (Map.Entry<String, String> entry : entryMap.entrySet()) {
//              switch (entry.getKey()) {
//                case "dotmode":
//                case "multiplyOp":
//                  optDM.put(entry.getKey(), entry.getValue());
//                  break;
//                default:
//                  log.warn("Unrecognized option: " + prefix + '.' + entry);
//                  break;
//              }
              if (prefix.isEmpty())
                optDM.put(entry.getKey(), entry.getValue());
              else
                optDM.put(prefix + '.'+entry.getKey(), entry.getValue());
            }
            break;
        }
      }
    }
    SortedKeyValueIterator<Key, Value> bottomIter;
    TwoTableIterator dmi = new TwoTableIterator();
    dmi.init(source, optDM, env);
    bottomIter = dmi;
//    Map<String, String> optSum = new HashMap<>();
//    optSum.put("all", "true");
//    SortedKeyValueIterator<Key,Value> sc = new BigDecimalCombiner.BigDecimalSummingCombiner();
//    sc.init(bottomIter, optSum, env);
//    bottomIter = sc;


    // for debugging
//    DebugInfoIterator debug = new DebugInfoIterator();
//    debug.init(bottomIter, Collections.<String,String>emptyMap(), env);
//    bottomIter = debug;


    if (optC.isEmpty()) {
      log.debug("Not configured to write to a table C with a BatchWriter.");
    } else {
      RemoteWriteIterator rwi = new RemoteWriteIterator();
      rwi.init(bottomIter, optC, env); // bottomIter is a SaveStateIterator!
      bottomIter = rwi;
    }
    return bottomIter;
  }
}
