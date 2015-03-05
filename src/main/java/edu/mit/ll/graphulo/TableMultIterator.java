package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.map.TransformedMap;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * SpGEMM on Accumulo tables.
 */
public class TableMultIterator extends BranchIterator implements OptionDescriber {
    private static final Logger log = LogManager.getLogger(TableMultIterator.class);

    public static final String PREFIX_R = "R";

    static final OptionDescriber.IteratorOptions iteratorOptions;
    static {
        final Map<String,String> optDesc = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
            optDesc.put(DotMultIterator.PREFIX_A+'.'+entry.getKey(), "Table A :"+entry.getValue());
        }
        for (Map.Entry<String, String> entry : RemoteSourceIterator.iteratorOptions.getNamedOptions().entrySet()) {
            optDesc.put(DotMultIterator.PREFIX_BT+'.'+entry.getKey(), "Table BT:"+entry.getValue());
        }

        iteratorOptions = new OptionDescriber.IteratorOptions("DotMultIterator",
                "Dot product on rows from Accumulo tables A and BT. Does not sum.",
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
        Map<String,String> optA = new HashMap<>(), optBT = new HashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(DotMultIterator.PREFIX_A))
                optA.put(key.substring(DotMultIterator.PREFIX_A.length()+1), entry.getValue());
            else if (key.startsWith(DotMultIterator.PREFIX_BT))
                optBT.put(key.substring(DotMultIterator.PREFIX_BT.length()+1), entry.getValue());
            else switch(key) {

                    default:
                        throw new IllegalArgumentException("unknown option: "+entry);
                }
        }
        return RemoteSourceIterator.validateOptionsStatic(optA) &&
                RemoteSourceIterator.validateOptionsStatic(optBT);
    }

    @Override
    public SortedKeyValueIterator<Key, Value> initBranchIterator(Map<String, String> options, IteratorEnvironment env) throws IOException {
        // parse options
        Map<String,String> optDM=new HashMap<>();//, optW=new HashMap<>();
        {
            Map<String, Map<String, String>> prefixMap = GraphuloUtil.splitMapPrefix(options);
            for (Map.Entry<String, Map<String, String>> prefixEntry : prefixMap.entrySet()) {
                final String prefix = prefixEntry.getKey();
                Map<String, String> entryMap = prefixEntry.getValue();

                switch (prefix) {
                    case DotMultIterator.PREFIX_A:
                    case DotMultIterator.PREFIX_BT: {
                        Map<String,String> withPrefix = new HashMap<>(entryMap.size());
                        for (Map.Entry<String, String> entry : entryMap.entrySet()) {
                            withPrefix.put(prefix + '.' + entry.getKey(), entry.getValue());
                        }
                        optDM.putAll(withPrefix);
                    }
                    break;
                    case PREFIX_R: {
                        //optW.putAll(entryMap);
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

        // this sets up the remote sources for now
        DotMultIterator dmi = new DotMultIterator();
        dmi.init(null, optDM, env);

        return dmi;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> initBranchAfterIterator(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        Map<String,String> optW=GraphuloUtil.splitMapPrefix(options).get(PREFIX_R);

        Map<String,String> optSum = new HashMap<>();
        optSum.put("all", "true");
        Combiner sc = new BigDecimalCombiner.BigDecimalSummingCombiner();
        sc.init(source, optSum, env);

        SortedKeyValueIterator<Key, Value> bottomIter;
        if (optW == null || optW.isEmpty()) {
            bottomIter = sc;
        }
        else {
            RemoteWriteIterator rwi = new RemoteWriteIterator();
            rwi.init(sc, optW, env);
            bottomIter = rwi;
        }
        return bottomIter;
    }


}
