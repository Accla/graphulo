package edu.mit.ll.graphulo;

import com.google.common.collect.Iterators;
import org.apache.accumulo.core.client.lexicoder.AbstractEncoder;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.nio.charset.StandardCharsets.UTF_8;

public class UtilTest {
  private static final Logger log = LogManager.getLogger(UtilTest.class);

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

  @Test
  public void testRangeSet() {
    RangeSet rs = new RangeSet();
    Range r;
    SortedSet<Range> targetRanges;

    r = new Range("b");
    Assert.assertEquals(r, Iterators.getOnlyElement(rs.iteratorWithRangeMask(r)));

    targetRanges = new TreeSet<>();
    targetRanges.add(new Range("b"));
    targetRanges.add(new Range("g"));
    rs.setTargetRanges(targetRanges);
    r = new Range("a", "d");
    Assert.assertEquals(targetRanges.first(), Iterators.getOnlyElement(rs.iteratorWithRangeMask(r)));

    r = new Range("a", "x");
    Assert.assertTrue(Iterators.elementsEqual(targetRanges.iterator(), rs.iteratorWithRangeMask(r)));
  }

  /** Small bug in Accumulo. */
  @Ignore("KnownBug: ACCUMULO-3900 Fixed in 1.7.1")
  @Test
  public void testAbstractEncoderDecode() {
    AbstractEncoder<Long> encoder = new LongCombiner.StringEncoder();
    byte[] bytes = "a334".getBytes();
    Assert.assertEquals(334, encoder.decode(bytes, 1, 3).longValue());
  }

  /** Comparing bytes from text objects and such. */
  @Test
  public void testTextCompare() {
    String s1 = "abcd", s2 = "ab";
    Text t1 = new Text(s1), t2 = new Text(s2);
    Assert.assertEquals(0, WritableComparator.compareBytes(t1.getBytes(), 0, t2.getLength(), t2.getBytes(), 0, t2.getLength()));
//    Assert.assertEquals(0, t1.compareTo(t2.getBytes(), 0, t2.getLength()));
    ByteSequence bs1 = new ArrayByteSequence(t1.getBytes());
  }

  @Test
  public void testKeyColQSubstring() {
    byte[] inBytes = "col".getBytes();
    Key k = new Key("row","colF","colQ");
    byte[] cqBytes = k.getColumnQualifierData().getBackingArray();
    Assert.assertEquals(0, WritableComparator.compareBytes(cqBytes, 0, inBytes.length, inBytes, 0, inBytes.length));
    String label = new String(cqBytes, inBytes.length, cqBytes.length-inBytes.length, UTF_8);
    Assert.assertEquals("Q",label);

    Assert.assertEquals("Q",GraphuloUtil.stringAfter(inBytes, cqBytes));
    Assert.assertEquals("colQ",GraphuloUtil.stringAfter("".getBytes(), cqBytes));
    Assert.assertEquals("colQ",GraphuloUtil.stringAfter(new byte[0], cqBytes));
    Assert.assertNull(GraphuloUtil.stringAfter("ca".getBytes(), cqBytes));
   }

  @Test
  public void testPrependStartPrefix() {
    char sep = ',';
    String startPrefix = "out|";
    String v0 = "v1,v3,v0,";
    Collection<Text> vktexts = GraphuloUtil.d4mRowToTexts(v0);
    String expect = "out|v1,out|v3,out|v0,";
    String actual = GraphuloUtil.prependStartPrefix(startPrefix, sep, vktexts);
    Set<String> expectSet = new HashSet<>(Arrays.asList(expect.split(","))),
        actualSet = new HashSet<>(Arrays.asList(actual.split(",")));
    Assert.assertEquals(expectSet, actualSet);

    expect = "out|,:,out},";
    Assert.assertEquals(expect, GraphuloUtil.prependStartPrefix(startPrefix, sep, null));
  }

  @Test
  public void testRowMiddle() {
    byte[] prefix = "pre|".getBytes();
    byte[] prefixMod = new byte[prefix.length];
    System.arraycopy(prefix,0,prefixMod,0,prefix.length-1);
    prefixMod[prefix.length-1] = (byte) (prefix[prefix.length-1]+1);

    log.debug("prefixMod="+new String(prefixMod));
    Range r = new Range(new String(prefix), true, new String(prefixMod), true);
    Assert.assertTrue(r.contains(new Key("pre|a")));
  }

  private void printArray(String header, byte[] arr) {
    System.out.print(header + ' ');
    for (byte b : arr)
      System.out.print(b+" ");
    System.out.println();
  }

  /** temporary */
  @Test
  public void test1() {
//    AbstractEncoder<Long> encoder = new ULongLexicoder();
//    printArray("1  ",encoder.encode(1l));
//    printArray("2  ",encoder.encode(2l));
//    printArray("10 ",encoder.encode(10l));
    byte[][] bs = new byte[3][];
    bs[0] = "abc".getBytes();
    bs[1] = "".getBytes();
    bs[2] = "xyz".getBytes();

    int totlen = 0;
    for (byte[] b : bs)
      totlen += b.length;
    byte[] ret = new byte[totlen];
    int pos = 0;
    for (byte[] b : bs) {
      System.arraycopy(b,0,ret,pos,b.length);
      pos += b.length;
    }

    Assert.assertEquals("abcxyz",new String(ret,UTF_8));
  }

  /** temporary */
  @Test
  public void test2() {
    byte[] a = "a".getBytes();
    byte[] b = new byte[a.length+1];
    System.arraycopy(a,0,b,0,a.length);
    b[a.length] = Byte.MAX_VALUE;

    Range r = new Range(new Text(a), new Text(b));
    Assert.assertTrue(r.contains(new Key("anfmkjdrngbukjrnfgkjrf")));
    Assert.assertTrue(r.contains(new Key("a")));
    Assert.assertTrue(r.contains(new Key("a\127")));
    Assert.assertTrue(r.contains(new Key("a2")));
    Assert.assertFalse(r.contains(new Key("b")));
  }

  @Test
  public void testPrependStartPrefix_D4MRange() {
    Assert.assertEquals("pre|a,pre|b,:,pre|v,:,",
        GraphuloUtil.padD4mString("pre|","","a,b,:,v,:,"));
    Assert.assertEquals("pre|aX,pre|bX,:,pre|vX,:,",
        GraphuloUtil.padD4mString("pre|", "X", "a,b,:,v,:,"));
    Assert.assertEquals(":,a,b,",
        GraphuloUtil.padD4mString(null, null, ":,a,b,"));
    Assert.assertEquals(":,ax,bx,",
        GraphuloUtil.padD4mString("", "x", ":,a,b,"));
  }

  @Test
  public void testMakeRangesD4mString() {
    Collection<Text> c = new ArrayList<>();
    c.add(new Text("v1|"));
    c.add(new Text("v5|"));
    Assert.assertEquals("v1|,:,v1|" + GraphuloUtil.LAST_ONE_BYTE_CHAR + ",v5|,:,v5|" + GraphuloUtil.LAST_ONE_BYTE_CHAR+",",
        GraphuloUtil.makeRangesD4mString(c, ','));
    Assert.assertEquals("v1|,:,v1|" + GraphuloUtil.LAST_ONE_BYTE_CHAR + ",v5|,:,v5|" + GraphuloUtil.LAST_ONE_BYTE_CHAR+",",
        GraphuloUtil.makeRangesD4mString("v1|,v5|,"));

    Collection<Range> rngs = GraphuloUtil.d4mRowToRanges(GraphuloUtil.makeRangesD4mString("v1|,v5|,"));
    boolean ok = false;
    for (Range rng : rngs) {
      if (rng.contains(new Key("v1|zfdwefwserdfsd")))
        ok = true;
    }
    Assert.assertTrue(ok);

    Assert.assertEquals("v1,:,v3,",
        GraphuloUtil.makeRangesD4mString("v1,:,v3,"));
    for (char b : Character.toChars(Byte.MAX_VALUE))
      System.out.print(b);
    System.out.println("   OK char length "+Character.toChars(Byte.MAX_VALUE).length);
    String s = GraphuloUtil.makeRangesD4mString(":,v3,v5,v9,");
    byte[] b = s.getBytes();
    log.debug(Key.toPrintableString(b, 0, b.length, b.length));

    Collection<Range> set = GraphuloUtil.d4mRowToRanges(s, true);
    log.debug(set);
    String s2 = GraphuloUtil.rangesToD4MString(set,',');
    byte[] b2 = s.getBytes();
    log.debug(Key.toPrintableString(b, 0, b.length, b.length));
    // establish fixpoint
    Assert.assertEquals(s, s2);
  }

}
