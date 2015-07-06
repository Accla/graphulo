package edu.mit.ll.graphulo;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.skvi.DynamicIterator;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Solves the problem of running out of iterator priority spaces.
 * Bundles several iterators together in one.
 * @see edu.mit.ll.graphulo.skvi.DynamicIterator
 */
public class DynamicIteratorSetting {
  private Deque<IteratorSetting> iteratorSettingList = new LinkedList<>();

  public DynamicIteratorSetting prepend(IteratorSetting setting) {
    iteratorSettingList.addFirst(setting);
    return this;
  }

  public DynamicIteratorSetting append(IteratorSetting setting) {
    iteratorSettingList.addLast(setting);
    return this;
  }

  public DynamicIteratorSetting clear() {
    iteratorSettingList.clear();
    return this;
  }

  public Map<String,String> buildSettingMap() {
    return buildSettingMap("");
  }

  /** Add the prefix to every setting option. */
  public Map<String,String> buildSettingMap(String pre) {
    if (pre == null) pre = "";
    Map<String,String> map = new HashMap<>();
    int prio = 1;
    for (IteratorSetting setting : iteratorSettingList) {
      String prefix = pre+prio+"."+setting.getName()+".";
      map.put(prefix+"class", setting.getIteratorClass());
      for (Map.Entry<String, String> entry : setting.getOptions().entrySet()) {
        map.put(prefix+"opt."+entry.getKey(), entry.getValue());
      }
      prio++;
    }
    return map;
  }

  public IteratorSetting toIteratorSetting(int priority) {
    return new IteratorSetting(priority, DynamicIterator.class, buildSettingMap());
  }

  public IteratorSetting toIteratorSetting(int priority, String name) {
    return new IteratorSetting(priority, name, DynamicIterator.class, buildSettingMap());
  }

  public void addToScanner(ScannerBase scanner, int priority) {
    if (!iteratorSettingList.isEmpty())
      scanner.addScanIterator(toIteratorSetting(priority));
  }

  public void addToScanner(ScannerBase scanner, int priority, String name) {
    if (!iteratorSettingList.isEmpty())
      scanner.addScanIterator(toIteratorSetting(priority, name));
  }

  public List<IteratorSetting> getIteratorSettingList() {
    return new ArrayList<>(iteratorSettingList);
  }

  /** Prefix is "".
   * @see #fromMap(String, Map)
   */
  public static DynamicIteratorSetting fromMap(Map<String,String> mapOrig) {
    return fromMap("", mapOrig);
  }

  /**
   * Load a DynamicIteratorSetting from a Map&lt;String,String&gt;.
   * Used inside the Accumulo iterator stack {@link SortedKeyValueIterator#init}.
   * @param pre A prefix that must be in front of every option, like "diter."
   * @param mapOrig A copy is made so that the original options are not consumed.
   * @return New DynamicIteratorSetting
   */
  public static DynamicIteratorSetting fromMap(String pre, Map<String,String> mapOrig) {
    if (pre == null) pre = "";
    Map<String,String> mapCopy = new LinkedHashMap<>(mapOrig);
    DynamicIteratorSetting dis = new DynamicIteratorSetting();
    for (int prio = 1; true; prio++) {
      String prioPrefix = prio+".";
      String clazz = null, name = null, clazzStr = null, optPrefix = null;
      Map<String,String> opt = new HashMap<>();

      for (Iterator<Map.Entry<String, String>> iterator = mapCopy.entrySet().iterator(); iterator.hasNext(); ) {
        Map.Entry<String, String> entry = iterator.next();
        String key = entry.getKey();
        if (!key.startsWith(pre))
          continue;
        key = key.substring(pre.length());

        if (name == null && key.startsWith(prioPrefix)) {
          int idxSecondDot = key.indexOf('.', prioPrefix.length());
          Preconditions.checkArgument(idxSecondDot != -1, "invalid map entry %s -> %s", key, entry.getValue());
          name = key.substring(prioPrefix.length(), idxSecondDot);
          clazzStr = prioPrefix + name + ".class";
          optPrefix = prioPrefix + name + ".opt.";
        }

        if (name != null && key.equals(clazzStr)) {
          Preconditions.checkArgument(clazz == null, "Class defined twice: %s -> %s", key, entry.getValue());
          clazz = entry.getValue();
          iterator.remove();
        } else if (name != null && key.startsWith(optPrefix)) {
          opt.put(key.substring(optPrefix.length()), entry.getValue());
          iterator.remove();
        }
      }
      if (name == null)
        break;
      Preconditions.checkArgument(clazz != null, "no class for IteratorSetting with name %s and options %s", name, opt);
      dis.append(new IteratorSetting(prio, name, clazz, opt));
    }
    return dis;
  }

  @Override
  public String toString() {
    return iteratorSettingList.toString();
  }

  /**
   * Used inside the Accumulo iterator stack to create the iterator list held in this object.
   * @see org.apache.accumulo.core.iterators.IteratorUtil#loadIterators
   * @return The iterators this object holds, loaded in order.
   */
  @SuppressWarnings("unchecked")
  public SortedKeyValueIterator<Key,Value> loadIteratorStack(SortedKeyValueIterator<Key,Value> source, IteratorEnvironment env) throws IOException {
    for (IteratorSetting setting : iteratorSettingList) {
      SortedKeyValueIterator<Key,Value> iter =
          (SortedKeyValueIterator<Key,Value>)GraphuloUtil.subclassNewInstance(
              setting.getIteratorClass(), SortedKeyValueIterator.class);
      Map<String,String> optOrig = setting.getOptions();            // the options are unmodifiable....
      Map<String,String> optCopy = new HashMap<>(optOrig.size());   // make a defensive copy so that the init function can modify them if it wants
      optCopy.putAll(optOrig);
      iter.init(source, optCopy, env);
      source = iter;
    }
    return source;
  }

}
