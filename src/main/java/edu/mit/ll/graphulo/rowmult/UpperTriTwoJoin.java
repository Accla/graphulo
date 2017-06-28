package edu.mit.ll.graphulo.rowmult;

import edu.mit.ll.graphulo.util.PeekingIterator1;
import edu.mit.ll.graphulo.util.SKVIRowIteratorNoValues;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;

import static edu.mit.ll.graphulo.rowmult.CartesianRowMultiply.readRowColumnsNoValues;

/**
 * TableMult: (k,cq,v) * (k,cq',v') = (cq,cq',2)
 * only when cq < cq'.
 */
public class UpperTriTwoJoin implements RowMultiplyOp {
  private static final Logger log = LogManager.getLogger(UpperTriTwoJoin.class);

  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
  }

  private static final Text EMPTY_TEXT = new Text();
  private static final TypedValueCombiner.Encoder<Integer> LEX = new IntegerLexicoder();
  private static final byte[] ZERO_BYTE = new byte[] { 0x00 };
  private static final Value VALUE_TWO = new Value(LEX.encode(2));

  @Override
  public Iterator<Map.Entry<Key, Value>> multiplyRow(
      SortedKeyValueIterator<Key, Value> skviA, SortedKeyValueIterator<Key, Value> skviB) throws IOException {
    assert skviA != null && skviB != null;

//    Text rowKmer = skviA.getTopKey().getRow();
    final PeekingIterator1<Key> itAonce = new PeekingIterator1<>(new SKVIRowIteratorNoValues(skviA));
    final SortedSet<Text> BrowMap = readRowColumnsNoValues(skviB);

    return new UpperTriTwoIterator(itAonce, BrowMap);
  }

  private class UpperTriTwoIterator implements Iterator<Map.Entry<Key, Value>> {
//    private static final Logger log = LogManager.getLogger(UpperTriTwoIterator.class);

    private final SortedSet<Text> BrowMap;
    private final PeekingIterator1<Key> itAonce;
    private Iterator<Text> itBreset;

    private Map.Entry<Key, Value> nextEntry;

    private final Text textAfterColumn = new Text();

    private Text getTextAfterColumn() {
      byte[] obs = itAonce.peek().getColumnQualifierData().toArray();
      textAfterColumn.set(obs);
      textAfterColumn.append(ZERO_BYTE, 0, 1);
      return textAfterColumn;
    }

    public UpperTriTwoIterator(PeekingIterator1<Key> itAonce, SortedSet<Text> mapBreset) {
      BrowMap = mapBreset;
      this.itAonce = itAonce;
      if (!itAonce.hasNext()) {
        nextEntry = null;
        return;
      }
      this.itBreset = BrowMap.tailSet(getTextAfterColumn()).iterator();
      if (itBreset.hasNext())
        prepNext();
      else {
        nextEntry = null;
      }
    }

    @Override
    public boolean hasNext() {
      return nextEntry != null;
    }

    @Override
    public Map.Entry<Key, Value> next() {
      Map.Entry<Key, Value> ret = nextEntry;
      nextEntry = null;
      if (itBreset.hasNext())
        prepNext();
      return ret;
    }

    private void prepNext() {
      Key eA;
      Text eB = itBreset.next();
      if (!itBreset.hasNext()) {
        eA = itAonce.next();    // advance itA
        if (itAonce.hasNext())  // STOP if no more itA
          itBreset = BrowMap.tailSet(getTextAfterColumn()).iterator();
        else
          return;
      } else
        eA = itAonce.peek();

      Text cola = eA.getColumnQualifier();
      Key nk = new Key(cola, EMPTY_TEXT, eB);
//      long a = LEX.decode(eA.getValue().get());
//      long b = LEX.decode(eB.getValue().get());

//      double nd = Math.min(((double)a)/da, ((double)b)/db); /// (da * db); // full calc is 1 - 2*
//      Value nv = new Value(LEXDOUBLE.encode(nd)); //new Value(Double.toString(nd).getBytes(UTF_8));

//      log.info("LowerTiJoin emits "+nk.toStringNoTime()+" value "+VALUE_TWO);

      nextEntry = new AbstractMap.SimpleImmutableEntry<>(nk, VALUE_TWO); // need to copy?
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }




}
