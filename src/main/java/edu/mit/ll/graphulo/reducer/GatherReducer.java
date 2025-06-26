package edu.mit.ll.graphulo.reducer;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Gather the unique values of part of a Key into a set.
 */
// could generalize to have multiply KeyParts
public class GatherReducer extends ReducerSerializable<HashSet<String>> {

  public enum KeyPart { ROW, COLF, COLQ, COLVIS, VAL}

  public static final String KEYPART = "KeyPart";

  public static Map<String,String> reducerOptions(KeyPart keyPart) {
    Map<String,String> opts = new HashMap<>();
    opts.put(KEYPART, keyPart.name());
    return opts;
  }

  private KeyPart keyPart;
  private HashSet<String> set = new HashSet<>();
  private Text tmpText = new Text();

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env)  {
    Preconditions.checkArgument(options.containsKey(KEYPART), "Must contain option " + KEYPART);
    keyPart = KeyPart.valueOf(options.get(KEYPART));
  }

  @Override
  public void reset() throws IOException {
    set.clear();
  }

  @Override
  public void update(Key k, Value v) {
    switch (keyPart) {
      case ROW:
        set.add(k.getRow(tmpText).toString());
        break;
      case COLF:
        set.add(k.getColumnFamily(tmpText).toString());
        break;
      case COLQ:
        set.add(k.getColumnQualifier(tmpText).toString());
        break;
      case COLVIS:
        set.add(k.getColumnVisibility(tmpText).toString());
        break;
      case VAL:
        set.add(v.toString());
        break;
      default:
        throw new AssertionError("no such KeyPart: "+keyPart);
    }
  }

  @Override
  public void combine(HashSet<String> another) {
    set.addAll(another);
  }

  @Override
  public boolean hasTopForClient() {
    return !set.isEmpty();
  }


  @Override
  public HashSet<String> getSerializableForClient() {
    return set;
  }

}
