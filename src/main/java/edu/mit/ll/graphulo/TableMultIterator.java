package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * SpGEMM on Accumulo tables.
 * If table R is not given, then no RemoteWriteIterator is used.
 * If table C is not given, then no C-summing RemoteSourceIterator is used.
 */
public class TableMultIterator extends BranchIterator implements OptionDescriber {
  private static final Logger log = LogManager.getLogger(TableMultIterator.class);

  public static final String PREFIX_R = "R";
  public static final String PREFIX_C = "C";

  static final OptionDescriber.IteratorOptions iteratorOptions;

  static {
    final Map<String, String> optDesc = new LinkedHashMap<>();
    optDesc.put("trace", "Use tracer? true or false");
    for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
      optDesc.put(DotMultIterator.PREFIX_A + '.' + entry.getKey(), "Table A :" + entry.getValue());
    }
    for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
      optDesc.put(DotMultIterator.PREFIX_BT + '.' + entry.getKey(), "Table BT:" + entry.getValue());
    }
    for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
      optDesc.put(PREFIX_C + '.' + entry.getKey(), "[Optional] Table C:" + entry.getValue());
    }
    for (Map.Entry<String, String> entry : RemoteWriteIterator.iteratorOptions.getNamedOptions().entrySet()) {
      optDesc.put(PREFIX_R + '.' + entry.getKey(), "[Optional] Table R:" + entry.getValue());
    }

    iteratorOptions = new OptionDescriber.IteratorOptions("DotMultIterator",
        "Multiply Accumulo tables A and BT, optionally summing in entries from C, writing to R.",
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
    Map<String, String> optA = new HashMap<>(), optBT = new HashMap<>(),
        optR = new HashMap<>(), optC = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(DotMultIterator.PREFIX_A))
        optA.put(key.substring(DotMultIterator.PREFIX_A.length() + 1), entry.getValue());
      else if (key.startsWith(DotMultIterator.PREFIX_BT))
        optBT.put(key.substring(DotMultIterator.PREFIX_BT.length() + 1), entry.getValue());
      else if (key.startsWith(PREFIX_R))
        optR.put(key.substring(PREFIX_R.length() + 1), entry.getValue());
      else if (key.startsWith(PREFIX_C))
        optC.put(key.substring(PREFIX_C.length() + 1), entry.getValue());
      else switch (key) {
          case "trace":
            //noinspection ResultOfMethodCallIgnored
            Boolean.parseBoolean(entry.getValue());
            break;
          default:
            throw new IllegalArgumentException("unknown option: " + entry);
        }
    }
    return RemoteSourceIterator.validateOptionsStatic(optA) &&
        RemoteSourceIterator.validateOptionsStatic(optBT) &&
        (optR.isEmpty() //|| (optR.containsKey("tableName") && optR.size() == 1)
             || RemoteWriteIterator.validateOptionsStatic(optR)) &&
        (optC.isEmpty() //|| (optC.containsKey("tableName") && optC.size() == 1)
             || RemoteSourceIterator.validateOptionsStatic(optC));
  }

  @Override
  public SortedKeyValueIterator<Key, Value> initBranchIterator(Map<String, String> options, IteratorEnvironment env) throws IOException {
    // parse options
    Map<String, String> optDM = new HashMap<>(), optC=new HashMap<>();
    {
      Map<String, Map<String, String>> prefixMap = GraphuloUtil.splitMapPrefix(options);
      for (Map.Entry<String, Map<String, String>> prefixEntry : prefixMap.entrySet()) {
        final String prefix = prefixEntry.getKey();
        Map<String, String> entryMap = prefixEntry.getValue();

        switch (prefix) {
          case DotMultIterator.PREFIX_A:
          case DotMultIterator.PREFIX_BT: {
            optDM.putAll(GraphuloUtil.preprendPrefixToKey(prefix + '.', entryMap));
            break;
          }
          case PREFIX_R: {
            //optW.putAll(entryMap);
            break;
          }
          case PREFIX_C: {
            optC.putAll(GraphuloUtil.preprendPrefixToKey(prefix + '.', entryMap));
            break;
          }
          default:
            for (Map.Entry<String, String> entry : entryMap.entrySet()) {
              log.warn("Unrecognized option: " + prefix + '.' + entry);
            }
            break;
        }
      }
    }
    SortedKeyValueIterator<Key, Value> bottomIter;

    // this sets up the remote sources
    DotMultIterator dmi = new DotMultIterator();
    dmi.init(null, optDM, env);
    bottomIter = dmi;

    if (!optC.isEmpty()) {
      RemoteSourceIterator csrc = new RemoteSourceIterator();
      csrc.init(null, optC, env);

      List<SortedKeyValueIterator<Key, Value>> list = new ArrayList<>(2);
      list.add(bottomIter);
      list.add(csrc);
      bottomIter = new MultiIterator(list, false);
    } else
      log.debug("Not configured to read and sum in a table C.");

    return bottomIter;
  }

  @Override
  public SortedKeyValueIterator<Key, Value> initBranchAfterIterator(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    Map<String, String> optW = GraphuloUtil.splitMapPrefix(options).get(PREFIX_R);

    Map<String, String> optSum = new HashMap<>();
    optSum.put("all", "true");
    Combiner sc = new BigDecimalCombiner.BigDecimalSummingCombiner();
    sc.init(source, optSum, env);

    SortedKeyValueIterator<Key, Value> bottomIter;
    if (optW == null || optW.isEmpty()) {
      log.debug("Not configured to write to a table R with a BatchWriter.");
      bottomIter = sc;
    } else {
      RemoteWriteIterator rwi = new RemoteWriteIterator();
      rwi.init(sc, optW, env);
      bottomIter = rwi;
    }
    return bottomIter;
  }


}
