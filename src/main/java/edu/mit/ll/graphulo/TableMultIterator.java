package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * SpGEMM on Accumulo tables: C += A * B. Requires transpose(A)=AT is stored as a separate table.
 * Run on table B and pass as an option the connection information to table AT. Or vice versa.
 * Reads from remote table iterator and from local iterator, performs outer product,
 *   sends partial products to result table C.
 * If C is not given, then emits the partial products as part of the query instead of using a BatchWriter.
 */
public class TableMultIterator extends BranchIterator implements OptionDescriber {
  private static final Logger log = LogManager.getLogger(TableMultIterator.class);
  public static final String PREFIX_C = "C";

  static final IteratorOptions iteratorOptions;
  static {
    final Map<String, String> optDesc = new LinkedHashMap<>();
    optDesc.put("trace", "Output timings on stdout?");
    for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
      optDesc.put(DotIterator.PREFIX_AT+ '.' + entry.getKey(), "Table AT:" + entry.getValue());
    }
    for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
      optDesc.put(PREFIX_C + '.' + entry.getKey(), "Table C:" + entry.getValue());
    }
    iteratorOptions = new IteratorOptions("TableMultIteratorQuery",
        "Outer product remote table transpose with local table, optionally sending partial products to C instead of output.",
        optDesc, null);
  }

  @Override
  public IteratorOptions describeOptions() {
    return iteratorOptions;
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    return validateOptionsStatic(options);
  }

  public static boolean validateOptionsStatic(Map<String, String> options) {
    Map<String, String> optAT = new HashMap<>(), optC = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(DotIterator.PREFIX_AT))
        optAT.put(key.substring(DotIterator.PREFIX_AT.length() + 1), entry.getValue());
      else if (key.startsWith(PREFIX_C))
        optC.put(key.substring(PREFIX_C.length() + 1), entry.getValue());
      else switch (key) {
          case "dot":
          case "multiplyOp":
//            optDM.put(entry.getKey(), entry.getValue());
            break;
          default:
            throw new IllegalArgumentException("unknown option: " + entry);
        }
    }
    return RemoteSourceIterator.validateOptionsStatic(optAT) &&
        (optC.isEmpty() || RemoteWriteIterator.validateOptionsStatic(optC));
  }

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
          case DotIterator.PREFIX_AT:
          case DotIterator.PREFIX_B: {
//            if (!optDM.isEmpty())
//              throw new IllegalArgumentException("Please specify either AT or B but not both.");
            optDM.putAll(GraphuloUtil.preprendPrefixToKey(prefix + '.', entryMap));
            break;
          }
          case PREFIX_C: {
            optC.putAll(entryMap);
            break;
          }
          default:
            for (Map.Entry<String, String> entry : entryMap.entrySet()) {
              switch (entry.getKey()) {
                case "dot":
                case "multiplyOp":
                  optDM.put(entry.getKey(), entry.getValue());
                  break;
                default:
                  log.warn("Unrecognized option: " + prefix + '.' + entry);
                  break;
              }
            }
            break;
        }
      }
    }
    SortedKeyValueIterator<Key, Value> bottomIter;
    DotIterator dmi = new DotIterator();
    dmi.init(source, optDM, env);
    bottomIter = dmi;
//    Map<String, String> optSum = new HashMap<>();
//    optSum.put("all", "true");
//    SortedKeyValueIterator<Key,Value> sc = new BigDecimalCombiner.BigDecimalSummingCombiner();
//    sc.init(bottomIter, optSum, env);
//    bottomIter = sc;
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
