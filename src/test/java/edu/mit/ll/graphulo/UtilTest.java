package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class UtilTest {

  /**
   * Retained in case it is useful again.
   */
  static class ColFamilyQualifierComparator implements Comparator<Key> {
    private Text text = new Text();

    @Override
    public int compare(Key k1, Key k2) {
      k2.getColumnFamily(text);
      int cfam = k1.compareColumnFamily(text);
      if (cfam != 0)
        return cfam;
      k2.getColumnQualifier(text);
      return k1.compareColumnQualifier(text);
    }
  }

  @Test
  public void testSortedMapComparator() {
    Key k1 = new Key("row1", "colF1", "colQ1");
    Key k2 = new Key("row2", "colF1", "colQ1");
    Key k3 = new Key("row3", "colF1", "colQ1");
    SortedMap<Key, Integer> map = new TreeMap<>(new ColFamilyQualifierComparator());
    map.put(k1, 1);
    map.put(k2, 2);
    int v = map.get(k3);
    Assert.assertEquals(2, v);
  }

  @Test
  public void testSplitMapPrefix() {
    Map<String, String> map = new HashMap<>();
    map.put("A.bla", "123");
    map.put("A.bla2", "345");
    map.put("B.ok", "789");
    map.put("plain", "vanilla");

    Map<String, Map<String, String>> expect = new HashMap<>();
    Map<String, String> m1 = new HashMap<>();
    m1.put("bla", "123");
    m1.put("bla2", "345");
    expect.put("A", m1);
    expect.put("B", Collections.singletonMap("ok", "789"));
    expect.put("", Collections.singletonMap("plain", "vanilla"));

    Map<String, Map<String, String>> actual = GraphuloUtil.splitMapPrefix(map);
    Assert.assertEquals(expect, actual);
  }

  @Test
  public void testPeekingIterator2() {
    List<Integer> list = new ArrayList<>();
    list.add(1);
    list.add(2);
    list.add(3);
    list.add(4);
    Iterator<Integer> iFirst = list.iterator(), iSecond = list.iterator();
    iSecond.next();
    PeekingIterator2<Integer> pe = new PeekingIterator2<>(list.iterator());
    while (pe.hasNext()) {
      Assert.assertTrue(iFirst.hasNext());
      Assert.assertEquals(iFirst.next(), pe.peekFirst());
      if (iSecond.hasNext())
        Assert.assertEquals(iSecond.next(), pe.peekSecond());
      else
        Assert.assertNull(pe.peekSecond());
      pe.next();
    }
    Assert.assertNull(pe.peekFirst());
  }

  @Test
  public void testd4mRowToRanges() {
    String rowStr;
    Collection<Range> actual, expect;

    {
      rowStr = "";
      expect = Collections.emptySet();
      actual = GraphuloUtil.d4mRowToRanges(rowStr);
      Assert.assertEquals(expect, actual);
    }
    {
      rowStr = "a\0";
      Key k = new Key("a");
      expect = Collections.singleton(new Range(k, true, k.followingKey(PartialKey.ROW), false));
      actual = GraphuloUtil.d4mRowToRanges(rowStr);
      Assert.assertEquals(expect, actual);
    }
    {
      rowStr = ":\7";
      expect = Collections.singleton(new Range());
      actual = GraphuloUtil.d4mRowToRanges(rowStr);
      Assert.assertEquals(expect, actual);
    }
    {
      rowStr = ":\7g\7";
      expect = Collections.singleton(new Range(null, false, "g", true));
      actual = GraphuloUtil.d4mRowToRanges(rowStr);
      Assert.assertEquals(expect, actual);
    }
    {
      rowStr = "a\0:\0";
      Key k = new Key("a");
      expect = Collections.singleton(new Range(k, true, null, false));
      actual = GraphuloUtil.d4mRowToRanges(rowStr);
      Assert.assertEquals(expect, actual);
    }
    {
      rowStr = "a\0:\0b\0";
      expect = Collections.singleton(new Range("a", true, "b", true));
      actual = GraphuloUtil.d4mRowToRanges(rowStr);
      Assert.assertEquals(expect, actual);
    }
    {
      rowStr = "a\0:\0b\0c\0";
      expect = new HashSet<>();
      expect.add(new Range("a", true, "b", true));
      Key k = new Key("c");
      expect.add(new Range(k, true, k.followingKey(PartialKey.ROW), false));
      actual = GraphuloUtil.d4mRowToRanges(rowStr);
      Assert.assertEquals(expect, actual);
    }
    {
      rowStr = "a\0:\0b\0c\0:\0";
      expect = new HashSet<>();
      expect.add(new Range("a", true, "b", true));
      expect.add(new Range("c", true, null, false));
      actual = GraphuloUtil.d4mRowToRanges(rowStr);
      Assert.assertEquals(expect, actual);
    }
    {
      rowStr = "a\0:\0b\0g\0c\0:\0";
      expect = new HashSet<>();
      expect.add(new Range("a", true, "b", true));
      // THIS OVERLAPS WITH RANGE [c,+inf)
      Key k = new Key("g");
      expect.add(new Range(k, true, k.followingKey(PartialKey.ROW), false));
      expect.add(new Range("c", true, null, false));
      actual = GraphuloUtil.d4mRowToRanges(rowStr);
      Assert.assertEquals(expect, actual);
    }
  }

  @Test
  public void testRangesToD4mRow() {
    String ex, ac;
    Collection<Range> in;
    final char sep = ',';

    {
      ex = "";
      in = Collections.emptySet();
      ac = GraphuloUtil.rangesToD4MString(in, sep);
      Assert.assertEquals(ex, ac);
      Assert.assertEquals(in, GraphuloUtil.d4mRowToRanges(GraphuloUtil.rangesToD4MString(in)));
    }
    {
      ex = "a,";
      Key k = new Key("a");
      in = Collections.singleton(new Range(k, true, k.followingKey(PartialKey.ROW), false));
      ac = GraphuloUtil.rangesToD4MString(in, sep);
      Assert.assertEquals(ex, ac);
      Assert.assertEquals(in, GraphuloUtil.d4mRowToRanges(GraphuloUtil.rangesToD4MString(in)));
      in = Collections.singleton(new Range("a"));
      ac = GraphuloUtil.rangesToD4MString(in, sep);
      Assert.assertEquals(ex, ac);
      Assert.assertEquals(in, GraphuloUtil.d4mRowToRanges(GraphuloUtil.rangesToD4MString(in)));
    }
    {
      ex = ":,";
      in = Collections.singleton(new Range());
      ac = GraphuloUtil.rangesToD4MString(in, sep);
      Assert.assertEquals(ex, ac);
      Assert.assertEquals(in, GraphuloUtil.d4mRowToRanges(GraphuloUtil.rangesToD4MString(in)));
    }
    {
      ex = ":,g,";
      in = Collections.singleton(new Range(null, false, "g", true));
      ac = GraphuloUtil.rangesToD4MString(in, sep);
      Assert.assertEquals(ex, ac);
      Assert.assertEquals(in, GraphuloUtil.d4mRowToRanges(GraphuloUtil.rangesToD4MString(in)));
      in = Collections.singleton(new Range(null, false, "g\0", false));
      ac = GraphuloUtil.rangesToD4MString(in, sep);
      Assert.assertEquals(ex, ac);
      Assert.assertEquals(in, GraphuloUtil.d4mRowToRanges(GraphuloUtil.rangesToD4MString(in)));
    }
    {
      ex = "a,:,";
      Key k = new Key("a");
      in = Collections.singleton(new Range(k, true, null, false));
      ac = GraphuloUtil.rangesToD4MString(in, sep);
      Assert.assertEquals(ex, ac);
      Assert.assertEquals(in, GraphuloUtil.d4mRowToRanges(GraphuloUtil.rangesToD4MString(in)));
    }
    {
      ex = "a\0,:,";
      Key k = new Key("a");
      in = Collections.singleton(new Range(k, false, null, false));
      ac = GraphuloUtil.rangesToD4MString(in, sep);
      Assert.assertEquals(ex, ac);
      in = Collections.singleton(new Range(k.followingKey(PartialKey.ROW), true, null, false));
      Assert.assertEquals(in, GraphuloUtil.d4mRowToRanges(GraphuloUtil.rangesToD4MString(in)));
    }
    {
      ex = "a,:,b,";
      in = Collections.singleton(new Range("a", true, "b", true));
      ac = GraphuloUtil.rangesToD4MString(in, sep);
      Assert.assertEquals(ex, ac);
      Assert.assertEquals(in, GraphuloUtil.d4mRowToRanges(GraphuloUtil.rangesToD4MString(in)));
    }
    {
      ex = "a,:,b,c,";
      in = new HashSet<>();
      in.add(new Range("a", true, "b", true));
      Key k = new Key("c");
      in.add(new Range(k, true, k.followingKey(PartialKey.ROW), false));
      ac = GraphuloUtil.rangesToD4MString(in, sep);
      Assert.assertEquals(ex, ac);
      Assert.assertEquals(in, GraphuloUtil.d4mRowToRanges(GraphuloUtil.rangesToD4MString(in)));
    }
    {
      ex = "a,:,b,c,:,";
      in = new HashSet<>();
      in.add(new Range("a", true, "b", true));
      in.add(new Range("c", true, null, false));
      ac = GraphuloUtil.rangesToD4MString(in, sep);
      Assert.assertEquals(ex, ac);
      Assert.assertEquals(in, GraphuloUtil.d4mRowToRanges(GraphuloUtil.rangesToD4MString(in)));
    }
    {
      ex = "a,:,b,g,x,:,";
      in = new HashSet<>();
      in.add(new Range("a", true, "b", true));
      Key k = new Key("g");
      in.add(new Range(k, true, k.followingKey(PartialKey.ROW), false));
      in.add(new Range("x", true, null, false));
      ac = GraphuloUtil.rangesToD4MString(in, sep);
      Assert.assertEquals(ex, ac);
      Assert.assertEquals(in, GraphuloUtil.d4mRowToRanges(GraphuloUtil.rangesToD4MString(in)));
    }
    {
      ex = "a,:,b,c,:,";
      in = new HashSet<>();
      in.add(new Range("a", true, "b", true));
      Key k = new Key("g");
      in.add(new Range(k, true, k.followingKey(PartialKey.ROW), false));
      in.add(new Range("c", true, null, false));
      ac = GraphuloUtil.rangesToD4MString(in, sep);
      Assert.assertEquals(ex, ac);
    }
  }


}
