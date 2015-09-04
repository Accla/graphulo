package edu.mit.ll.graphulo.apply;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * FlatMap across a list of Apply operations.
 */
public class MultiApply implements ApplyOp {
  private static final Logger log = LogManager.getLogger(MultiApply.class);

  private Deque<ApplyOp> applyOps;

  public static Map<String,String> buildSettingMap(String pre, List<Class<? extends ApplyOp>> classes,
                                                   List<Map<String,String>> optionMaps) {
    Preconditions.checkNotNull(classes);
    Preconditions.checkNotNull(optionMaps);
    Preconditions.checkArgument(classes.size() == optionMaps.size(), "must include an option map (even an empty one) for every Applyop class");
    if (pre == null) pre = "";
    Map<String,String> map = new HashMap<>();
    Iterator<Class<? extends ApplyOp>> classIterator = classes.iterator();
    Iterator<Map<String, String>> optionMapIterator = optionMaps.iterator();
    int prio = 1;
    while (classIterator.hasNext()) {
      Class<? extends ApplyOp> clazz = classIterator.next();
      Map<String, String> optionMap = optionMapIterator.next();
      String prefix = pre + prio + "." + clazz.getSimpleName() + ".";
      map.put(prefix + "class", clazz.getName());        // 1.itername.class -> classname
      for (Map.Entry<String, String> entry : optionMap.entrySet()) {
        map.put(prefix + "opt." + entry.getKey(), entry.getValue());  // 1.itername.opt.optkey -> optvalue
      }
      prio++;
    }
    return map;
  }

  private void fromMap(String pre, Map<String,String> mapOrig, IteratorEnvironment env) {
    applyOps = new ArrayDeque<>();
    if (pre == null) pre = "";
    Map<String,String> mapCopy = new HashMap<>(mapOrig);
    for (int prio = 1; true; prio++) {
      String prioPrefix = prio+".";
      String clazz = null, name = null, clazzStr = null, optPrefix = null;
      Map<String,String> opt = new HashMap<>();

      for (Iterator<Map.Entry<String, String>> iterator = mapCopy.entrySet().iterator(); iterator.hasNext(); ) {
        Map.Entry<String, String> entry = iterator.next();
        String key = entry.getKey();
        if (!key.startsWith(pre)) {
          iterator.remove();
          continue;
        }
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
      Preconditions.checkArgument(clazz != null, "no class for ApplyOp with name %s and options %s", name, opt);
      applyOps.add(loadApplyOp(clazz, opt, env));
    }
  }

  private ApplyOp loadApplyOp(String clazz, Map<String, String> opt, IteratorEnvironment env) {
    ApplyOp applyOp = GraphuloUtil.subclassNewInstance(clazz, ApplyOp.class);
    try {
      applyOp.init(opt, env);
    } catch (IOException e) {
      throw new RuntimeException("problem creating ApplyOp "+applyOp, e);
    }
    return applyOp;
  }

  /** Create an IteratorSetting that prunes every Key it sees.
   * A null <tt>pk</tt> means reduce the Key to the seek start Key (which is the all empty fields Key if seek range starts at -inf). */
  public static IteratorSetting iteratorSetting(int priority, List<Class<? extends ApplyOp>> classes,
                                                List<Map<String,String>> optionMaps) {
    IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
    itset.addOption(ApplyIterator.APPLYOP, MultiApply.class.getName());
    itset.addOptions(buildSettingMap(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX, classes, optionMaps));
    return itset;
  }

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    fromMap("", options, env);
  }

  @Override
  public Iterator<? extends Map.Entry<Key, Value>> apply(Key ok, Value ov) throws IOException {
    if (applyOps.size() == 1)
      return applyOps.getFirst().apply(ok, ov);
    Iterator<? extends Map.Entry<Key, Value>> iteratorPrev = Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(ok, ov)),
        iteratorNext = null;
    for (ApplyOp applyOp : applyOps) {
      assert iteratorPrev != null;
      while (iteratorPrev.hasNext()) {
        Map.Entry<Key, Value> entry = iteratorPrev.next();
        iteratorNext = iteratorNext == null ? applyOp.apply(entry.getKey(), entry.getValue())
            : Iterators.concat(iteratorNext, applyOp.apply(entry.getKey(), entry.getValue()));
      }
      iteratorPrev = iteratorNext;
      iteratorNext = null;
    }
    return iteratorPrev;
  }

  @Override
  public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    for (ApplyOp applyOp : applyOps) {
      applyOp.seekApplyOp(range, columnFamilies, inclusive);
    }
  }
}
