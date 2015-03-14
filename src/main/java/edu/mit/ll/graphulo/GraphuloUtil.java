package edu.mit.ll.graphulo;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility functions
 */
public class GraphuloUtil {

    /**
     * Split options on period characters.
     * "" holds entries without a period.
     * @return Map Prefix -> (Map entryWithoutPrefix -> value)
     */
    public static Map<String,Map<String,String>> splitMapPrefix(Map<String,String> options) {
        Map<String,Map<String,String>> prefixMap = new HashMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            int periodIdx = key.indexOf(".");
            if (periodIdx == 0)
                throw new IllegalArgumentException("not allowed to have option that starts with period: "+entry);
            String prefix = periodIdx == -1
                            ? "" : key.substring(0,periodIdx);
            String afterPrefix = periodIdx == -1
                                 ? key : key.substring(periodIdx+1); // ok if empty

            Map<String, String> mapInside = prefixMap.get(prefix);
            if (mapInside == null) {
                mapInside = new HashMap<>();
            }
            mapInside.put(afterPrefix, entry.getValue());
            prefixMap.put(prefix, mapInside);
        }
        return prefixMap;
    }

  public static Map<String,String> preprendPrefixToKey(String prefix, Map<String,String> options) {
    Map<String,String> res = new HashMap<>(options.size());
    for (Map.Entry<String, String> entry : options.entrySet()) {
      res.put(prefix+entry.getKey(), entry.getValue());
    }
    return res;
  }
}
