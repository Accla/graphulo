package edu.mit.ll.graphulo;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.apply.ApplyOp;
import edu.mit.ll.graphulo.apply.MultiApply;
import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.skvi.D4mRangeFilter;
import edu.mit.ll.graphulo.skvi.MapIterator;
import edu.mit.ll.graphulo.skvi.MinMaxFilter;
import edu.mit.ll.graphulo.skvi.NoConsecutiveDuplicateRowsIterator;
import edu.mit.ll.graphulo.skvi.TopColPerRowIterator;
import edu.mit.ll.graphulo.skvi.TriangularFilter;
import edu.mit.ll.graphulo.util.DoubletonIterator;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import edu.mit.ll.graphulo.util.PeekingIterator2;
import edu.mit.ll.graphulo.util.RangeSet;
import edu.mit.ll.graphulo.util.SerializationUtil;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.lexicoder.AbstractEncoder;
import org.apache.accumulo.core.client.mock.IteratorAdapter;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
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
import java.util.PriorityQueue;
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
    String startPrefix = "out|,";
    String v0 = "v1,v3,v0,";
    Collection<Text> vktexts = GraphuloUtil.d4mRowToTexts(v0);
    String expect = "out|v1,out|v3,out|v0,";
    String actual = Graphulo.prependStartPrefix(startPrefix, vktexts);
    Set<String> expectSet = new HashSet<>(Arrays.asList(GraphuloUtil.splitD4mString(expect))),
        actualSet = new HashSet<>(Arrays.asList(GraphuloUtil.splitD4mString(actual)));
    Assert.assertEquals(expectSet, actualSet);

    expect = "out|,:,out},";
    Assert.assertEquals(expect, Graphulo.prependStartPrefix(startPrefix, null));

    startPrefix = "out|,in|,";
    v0 = "v1,v3,v0,";
    vktexts = GraphuloUtil.d4mRowToTexts(v0);
    expect = "out|v1,out|v3,out|v0,in|v1,in|v3,in|v0,";
    actual = Graphulo.prependStartPrefix(startPrefix, vktexts);
    expectSet = new HashSet<>(Arrays.asList(GraphuloUtil.splitD4mString(expect)));
    actualSet = new HashSet<>(Arrays.asList(GraphuloUtil.splitD4mString(actual)));
    Assert.assertEquals(expectSet, actualSet);

    expect = "out|,:,out},in|,:,in},";
    Assert.assertEquals(expect, Graphulo.prependStartPrefix(startPrefix, null));

    startPrefix = "out|,in|,,";
    v0 = "v1,v3,v0,";
    vktexts = GraphuloUtil.d4mRowToTexts(v0);
    expect = "out|v1,out|v3,out|v0,in|v1,in|v3,in|v0,v1,v3,v0,";
    actual = Graphulo.prependStartPrefix(startPrefix, vktexts);
    expectSet = new HashSet<>(Arrays.asList(GraphuloUtil.splitD4mString(expect)));
    actualSet = new HashSet<>(Arrays.asList(GraphuloUtil.splitD4mString(actual)));
    Assert.assertEquals(expectSet, actualSet);
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

    Assert.assertEquals("abcxyz",new String(ret, StandardCharsets.UTF_8));
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
//    System.out.println(GraphuloUtil.prevRow(Range.followingPrefix(new Text("pre|")).toString()));
    Assert.assertEquals("pre|a,pre|b,:,pre|v,pre|z,:,pre|" + GraphuloUtil.LAST_ONE_BYTE_CHAR + ",",
        GraphuloUtil.padD4mString("pre|,", null, "a,b,:,v,z,:,"));
    Assert.assertEquals("pre|aX,pre|bX,:,pre|vX,pre|zX,:,pre|"+GraphuloUtil.LAST_ONE_BYTE_CHAR+"X,",
        GraphuloUtil.padD4mString("pre|,", "X,", "a,b,:,v,z,:,"));
    Assert.assertEquals(":,a,b,",
        GraphuloUtil.padD4mString(null, null, ":,a,b,"));
    Assert.assertEquals(":,ax,bx,",
        GraphuloUtil.padD4mString(null, "x,", ":,a,b,"));
    Assert.assertEquals("0,:,0a,0b,1,:,1a,1b,",
        GraphuloUtil.padD4mString("0,1,", null, ":,a,b,"));

    Assert.assertEquals("0a,0b,1a,1b,",
        GraphuloUtil.padD4mString("0,1,", null, "a,b,"));
    Assert.assertEquals("0ax,0bx,1ax,1bx,",
        GraphuloUtil.padD4mString("0,1,", "x,", "a,b,"));
  }

  @Test
  public void testMakeRangesD4mString() {
    Collection<Text> c = new ArrayList<>();
    c.add(new Text("v1|"));
    c.add(new Text("v5|"));
    Assert.assertEquals("v1|,:,v1|" + GraphuloUtil.LAST_ONE_BYTE_CHAR + ",v5|,:,v5|" + GraphuloUtil.LAST_ONE_BYTE_CHAR+",",
        GraphuloUtil.singletonsAsPrefix(c, ','));
    Assert.assertEquals("v1|,:,v1|" + GraphuloUtil.LAST_ONE_BYTE_CHAR + ",v5|,:,v5|" + GraphuloUtil.LAST_ONE_BYTE_CHAR+",",
        GraphuloUtil.singletonsAsPrefix("v1|,v5|,"));

    Collection<Range> rngs = GraphuloUtil.d4mRowToRanges(GraphuloUtil.singletonsAsPrefix("v1|,v5|,"));
    boolean ok = false;
    for (Range rng : rngs) {
      if (rng.contains(new Key("v1|zfdwefwserdfsd")))
        ok = true;
    }
    Assert.assertTrue(ok);

    Assert.assertEquals("v1,:,v3,",
        GraphuloUtil.singletonsAsPrefix("v1,:,v3,"));
    for (char b : Character.toChars(Byte.MAX_VALUE))
      System.out.print(b);
    System.out.println("   OK char length "+Character.toChars(Byte.MAX_VALUE).length);
    String s = GraphuloUtil.singletonsAsPrefix(":,v3,v5,v9,");
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

  @Test
  public void testDynamicIteratorSetting() {
    DynamicIteratorSetting dis = new DynamicIteratorSetting(5, null);
    dis.append(MinMaxFilter.iteratorSetting(1, MathTwoScalar.ScalarType.LONG, 5, null));
    dis.append(new IteratorSetting(1, MinMaxFilter.class, Collections.singletonMap("negate", Boolean.toString(true))));

    IteratorSetting setting1 = dis.toIteratorSetting();
    Map<String,String> mapCopy = new HashMap<>(setting1.getOptions());
    Assert.assertEquals(setting1,
        DynamicIteratorSetting.fromMap(mapCopy).toIteratorSetting());

    DynamicIteratorSetting dis2 = new DynamicIteratorSetting(5, null);
    dis2.append(TriangularFilter.iteratorSetting(1, TriangularFilter.TriangularType.Upper));
    dis2.prepend(setting1);
    IteratorSetting setting2 = dis2.toIteratorSetting();
    Assert.assertEquals(setting2,
        DynamicIteratorSetting.fromMap(setting2.getOptions()).toIteratorSetting());
    log.info("DynamicIteratorSetting2: " + setting2);
  }

  @Test
  public void testSplitD4mString() {
    String s;
    String[] e, a;

    s = "a,b,c,";
    e = new String[] {"a", "b", "c"};
    a = GraphuloUtil.splitD4mString(s);
    Assert.assertArrayEquals(e, a);

    s = "a,b,::::,,";
    e = new String[] {"a", "b", "::::", ""};
    a = GraphuloUtil.splitD4mString(s);
    Assert.assertArrayEquals(e, a);

    s = ",";
    e = new String[] {""};
    a = GraphuloUtil.splitD4mString(s);
    Assert.assertArrayEquals(e, a);
  }

  @Test
  public void testPrependPrefixToString() {
    Range r, e, a;
    String pre = "pre|";
    Assert.assertEquals("pre}", Range.followingPrefix(new Text("pre|")).toString());

    r = new Range("a",true,"b",true);
    e = new Range("pre|a",true,"pre|b",true);
    a = GraphuloUtil.prependPrefixToRange(pre, r);
    Assert.assertEquals(e, a);

    r = new Range("a",true,null,false);
    e = new Range("pre|a",true,"pre}",false);
    log.info(e);
    a = GraphuloUtil.prependPrefixToRange(pre, r);
    Assert.assertEquals(e, a);

    r = new Range(null,false,"b",true);
    e = new Range("pre|",true,"pre|b",true);
    a = GraphuloUtil.prependPrefixToRange(pre, r);
    Assert.assertEquals(e, a);

    r = new Range();
    e = new Range("pre|",true,"pre}",false);
    a = GraphuloUtil.prependPrefixToRange(pre, r);
    Assert.assertEquals(e, a);

    r = new Range("a",true,"b",true);
    pre = "";
    Assert.assertEquals(r, GraphuloUtil.prependPrefixToRange(pre, r));
  }

  @Test
  public void testDoubletonIterator() {
    DoubletonIterator<Integer> it = new DoubletonIterator<>();
    it.reuseAndReset(7, 8);
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(7, it.next().intValue());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(8, it.next().intValue());
    Assert.assertFalse(it.hasNext());
    it.reset();
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(7, it.next().intValue());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(8, it.next().intValue());
    Assert.assertFalse(it.hasNext());
    it.reuseAndReset(5, 6);
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(5, it.next().intValue());
    Assert.assertTrue(it.hasNext());
    Assert.assertEquals(6, it.next().intValue());
    Assert.assertFalse(it.hasNext());
  }

  @Test
  public void testPriorityQueueOrder() {
    class Entry implements Comparable<Entry> {
      Double k;
      Integer v;
      public Entry(Double k, Integer v) {
        this.k = k; this.v = v;
      }

      @Override
      public int compareTo(Entry o) {
        double diff = k - o.k;
        if (diff > 0) return 1;
        if (diff < 0) return -1;
        return 0;
      }
    }

    final PriorityQueue<Entry> pqs = new PriorityQueue<>();
    pqs.add(new Entry(5.4, 9));
    pqs.add(new Entry(6.4, 8));
    pqs.add(new Entry(7.4, 7));
    pqs.add(new Entry(1.4, 2));
    Assert.assertEquals(1.4, pqs.poll().k, 0.00001);
    Assert.assertEquals(5.4, pqs.poll().k, 0.00001);
    Assert.assertEquals(6.4, pqs.poll().k, 0.00001);
    Assert.assertEquals(7.4, pqs.poll().k, 0.00001);
    Assert.assertTrue(pqs.isEmpty());
  }

  @Test
  public void testNumD4mStr() {
    Assert.assertEquals(0, GraphuloUtil.NumD4mStr(null));
    Assert.assertEquals(0, GraphuloUtil.NumD4mStr(""));
    Assert.assertEquals(1, GraphuloUtil.NumD4mStr(","));
    Assert.assertEquals(2, GraphuloUtil.NumD4mStr(",,"));
    Assert.assertEquals(2, GraphuloUtil.NumD4mStr(",a,"));
    Assert.assertEquals(2, GraphuloUtil.NumD4mStr("a,b,"));
    Assert.assertEquals(3, GraphuloUtil.NumD4mStr("zcsazfcdsf,sgrsdgf,asxcawsd,"));
    Assert.assertEquals(4, GraphuloUtil.NumD4mStr("235trwgrt5h5;ewr;34rf;;"));
  }

  @Test
  public void testTopColPerRowIterator() throws IOException {
    Map<String,String> opts = TopColPerRowIterator.combinerSetting(1,3).getOptions();

    SortedMap<Key,Value> input = new TreeMap<>();
    input.put(new Key("r1", "", "c1"), new Value("4.5".getBytes()));
    input.put(new Key("r1", "", "c2"), new Value("6.0".getBytes()));
    input.put(new Key("r1", "", "c3"), new Value("5".getBytes()));
    input.put(new Key("r1", "", "c4"), new Value("1.1".getBytes()));
    input.put(new Key("r1", "", "c5"), new Value("8".getBytes()));

    input.put(new Key("r2", "", "c1"), new Value("13".getBytes()));
    input.put(new Key("r2", "", "c2"), new Value("12".getBytes()));

    input.put(new Key("r3", "", "c1"), new Value("13".getBytes()));
    input.put(new Key("r3", "", "c1"), new Value("19".getBytes()));
    input.put(new Key("r3", "", "c1"), new Value("11".getBytes()));
    input.put(new Key("r3", "", "c1"), new Value("10".getBytes()));
    input.put(new Key("r3", "", "c1"), new Value("12".getBytes()));

    SortedKeyValueIterator<Key,Value> skvi = new MapIterator(input);
    skvi.init(null, null, null);
    SortedKeyValueIterator<Key,Value> skviTop = new TopColPerRowIterator();
    skviTop.init(skvi, opts, null);
    skvi = skviTop;
    skvi.seek(new Range(), Collections.<ByteSequence>emptySet(), false);

    SortedMap<Key,Value> expect = new TreeMap<>();
    expect.put(new Key("r1", "", "c2"), new Value("6.0".getBytes()));
    expect.put(new Key("r1", "", "c3"), new Value("5".getBytes()));
    expect.put(new Key("r1", "", "c5"), new Value("8".getBytes()));
    expect.put(new Key("r2", "", "c1"), new Value("13".getBytes()));
    expect.put(new Key("r2", "", "c2"), new Value("12".getBytes()));
    expect.put(new Key("r3", "", "c1"), new Value("13".getBytes()));
    expect.put(new Key("r3", "", "c1"), new Value("19".getBytes()));
    expect.put(new Key("r3", "", "c1"), new Value("12".getBytes()));

    IteratorAdapter ia = new IteratorAdapter(skvi);
    for (Map.Entry<Key, Value> expectEntry : expect.entrySet()) {
      Assert.assertTrue(ia.hasNext());
      Map.Entry<Key, Value> actualEntry = ia.next();
      Assert.assertEquals(expectEntry, actualEntry);
//      System.out.println("MATCH "+expectEntry);
    }
    Assert.assertFalse(ia.hasNext());
  }

  @Test
  public void testNoConsecutiveDuplicateRowsIterator() throws IOException {
    SortedMap<Key,Value> input = new TreeMap<>();
    input.put(new Key("r1", "", "c1"), new Value("4.5".getBytes()));
    input.put(new Key("r1", "", "c2"), new Value("6.0".getBytes()));
    input.put(new Key("r1", "", "c3"), new Value("5".getBytes()));
    input.put(new Key("r1", "", "c4"), new Value("1.1".getBytes()));
    input.put(new Key("r1", "", "c5"), new Value("8".getBytes()));

    input.put(new Key("r3", "", "c1"), new Value("13".getBytes()));
    input.put(new Key("r3", "", "c2"), new Value("12".getBytes()));

    input.put(new Key("r4", "", "c1"), new Value("13".getBytes()));
    input.put(new Key("r4", "", "c1"), new Value("19".getBytes()));
    input.put(new Key("r4", "", "c1"), new Value("11".getBytes()));
    input.put(new Key("r4", "", "c1"), new Value("10".getBytes()));
    input.put(new Key("r4", "", "c1"), new Value("12".getBytes()));

    SortedMap<Key,Value> expect = new TreeMap<>();
    expect.putAll(input);

    input.put(new Key("r2", "", "c1"), new Value("4.5".getBytes()));
    input.put(new Key("r2", "", "c2"), new Value("6.0".getBytes()));
    input.put(new Key("r2", "", "c3"), new Value("5".getBytes()));
    input.put(new Key("r2", "", "c4"), new Value("1.1".getBytes()));
    input.put(new Key("r2", "", "c5"), new Value("8".getBytes()));

    SortedKeyValueIterator<Key,Value> skvi = new MapIterator(input);
    skvi.init(null, null, null);
    SortedKeyValueIterator<Key,Value> skviTop = new NoConsecutiveDuplicateRowsIterator();
    skviTop.init(skvi, Collections.<String, String>emptyMap(), null);
    skvi = skviTop;
    skvi.seek(new Range(), Collections.<ByteSequence>emptySet(), false);


    IteratorAdapter ia = new IteratorAdapter(skvi);
    for (Map.Entry<Key, Value> expectEntry : expect.entrySet()) {
      Assert.assertTrue(ia.hasNext());
      Map.Entry<Key, Value> actualEntry = ia.next();
      Assert.assertEquals(expectEntry, actualEntry);
//      System.out.println("MATCH "+expectEntry);
    }
    Assert.assertFalse(ia.hasNext());
  }

  @Test
  public void testD4mRangeFilter() throws IOException {
    SortedMap<Key,Value> input = new TreeMap<>();
    input.put(new Key("r1", "", "c1"), new Value("4.5".getBytes()));
    input.put(new Key("r1", "", "c2"), new Value("6.0".getBytes()));
    input.put(new Key("r1", "", "c3"), new Value("5".getBytes()));
    input.put(new Key("r1", "", "c4"), new Value("1.1".getBytes()));
    input.put(new Key("r1", "", "c5"), new Value("8".getBytes()));

    input.put(new Key("r2", "", "c1"), new Value("13".getBytes()));
    input.put(new Key("r2", "", "c2"), new Value("12".getBytes()));

    input.put(new Key("r3", "", "c1"), new Value("13".getBytes()));
    input.put(new Key("r3", "", "c1"), new Value("19".getBytes()));
    input.put(new Key("r3", "", "c1"), new Value("11".getBytes()));
    input.put(new Key("r3", "", "c1"), new Value("10".getBytes()));
    input.put(new Key("r3", "", "c1"), new Value("12".getBytes()));

    {
      SortedKeyValueIterator<Key, Value> skvi = new MapIterator(input);
      skvi.init(null, null, null);
      SortedKeyValueIterator<Key, Value> skviTop = new D4mRangeFilter();
      Map<String, String> opts = D4mRangeFilter.iteratorSetting(1, D4mRangeFilter.KeyPart.ROW, "r2,:,r3,").getOptions();
      skviTop.init(skvi, opts, null);
      skvi = skviTop;
      skvi.seek(new Range(), Collections.<ByteSequence>emptySet(), false);

      SortedMap<Key, Value> expect = new TreeMap<>();
      expect.put(new Key("r2", "", "c1"), new Value("13".getBytes()));
      expect.put(new Key("r2", "", "c2"), new Value("12".getBytes()));
      expect.put(new Key("r3", "", "c1"), new Value("13".getBytes()));
      expect.put(new Key("r3", "", "c1"), new Value("19".getBytes()));
      expect.put(new Key("r3", "", "c1"), new Value("11".getBytes()));
      expect.put(new Key("r3", "", "c1"), new Value("10".getBytes()));
      expect.put(new Key("r3", "", "c1"), new Value("12".getBytes()));

      IteratorAdapter ia = new IteratorAdapter(skvi);
      for (Map.Entry<Key, Value> expectEntry : expect.entrySet()) {
        Assert.assertTrue(ia.hasNext());
        Map.Entry<Key, Value> actualEntry = ia.next();
        Assert.assertEquals(expectEntry, actualEntry);
//      System.out.println("MATCH "+expectEntry);
      }
      Assert.assertFalse(ia.hasNext());
    }
    {
      SortedKeyValueIterator<Key, Value> skvi = new MapIterator(input);
      skvi.init(null, null, null);
      SortedKeyValueIterator<Key, Value> skviTop = new D4mRangeFilter();
      Map<String, String> opts = D4mRangeFilter.iteratorSetting(1, D4mRangeFilter.KeyPart.ROW, "r2,:,r3,", true).getOptions();
      skviTop.init(skvi, opts, null);
      skvi = skviTop;
      skvi.seek(new Range(), Collections.<ByteSequence>emptySet(), false);

      SortedMap<Key, Value> expect = new TreeMap<>();
      expect.put(new Key("r1", "", "c1"), new Value("4.5".getBytes()));
      expect.put(new Key("r1", "", "c2"), new Value("6.0".getBytes()));
      expect.put(new Key("r1", "", "c3"), new Value("5".getBytes()));
      expect.put(new Key("r1", "", "c4"), new Value("1.1".getBytes()));
      expect.put(new Key("r1", "", "c5"), new Value("8".getBytes()));

      IteratorAdapter ia = new IteratorAdapter(skvi);
      for (Map.Entry<Key, Value> expectEntry : expect.entrySet()) {
        Assert.assertTrue(ia.hasNext());
        Map.Entry<Key, Value> actualEntry = ia.next();
        Assert.assertEquals(expectEntry, actualEntry);
      }
      Assert.assertFalse(ia.hasNext());
    }
    {
      SortedKeyValueIterator<Key, Value> skvi = new MapIterator(input);
      skvi.init(null, null, null);
      SortedKeyValueIterator<Key, Value> skviTop = new D4mRangeFilter();
      Map<String, String> opts = D4mRangeFilter.iteratorSetting(1, D4mRangeFilter.KeyPart.VAL, "5,:,69,8,").getOptions();
      skviTop.init(skvi, opts, null);
      skvi = skviTop;
      skvi.seek(new Range(), Collections.<ByteSequence>emptySet(), false);

      SortedMap<Key, Value> expect = new TreeMap<>();
      expect.put(new Key("r1", "", "c2"), new Value("6.0".getBytes()));
      expect.put(new Key("r1", "", "c3"), new Value("5".getBytes()));
      expect.put(new Key("r1", "", "c5"), new Value("8".getBytes()));

      IteratorAdapter ia = new IteratorAdapter(skvi);
      for (Map.Entry<Key, Value> expectEntry : expect.entrySet()) {
        Assert.assertTrue(ia.hasNext());
        Map.Entry<Key, Value> actualEntry = ia.next();
        Assert.assertEquals(expectEntry, actualEntry);
      }
      Assert.assertFalse(ia.hasNext());
    }
  }

  public static class AppendApply implements ApplyOp {
    private String str;
    @Override
    public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
      str = options.get("str");
    }
    @Override
    public Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v) throws IOException {
      return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(
          k, new Value(v.toString().concat(str).getBytes())));
    }
    @Override
    public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    }
  }
  public static class DuplicateApply implements ApplyOp {
    @Override
    public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {
    }
    @Override
    public Iterator<? extends Map.Entry<Key, Value>> apply(Key k, Value v) throws IOException {
      return Iterators.concat(
          Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, new Value(v))),
          Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(k, new Value(v))));
    }
    @Override
    public void seekApplyOp(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    }
  }

  @Test
  public void testMultiApply() throws IOException {
    SortedMap<Key,Value> input = new TreeMap<>();
    input.put(new Key("r1", "", "c1"), new Value("a".getBytes()));
    input.put(new Key("r1", "", "c2"), new Value("b".getBytes()));
    input.put(new Key("r2", "", "c1"), new Value("c".getBytes()));

    String str = "X";
    SortedMap<Key,Value> expect = new TreeMap<>();
    expect.put(new Key("r1", "", "c1"), new Value("aX".getBytes()));
    expect.put(new Key("r1", "", "c2"), new Value("bX".getBytes()));
    expect.put(new Key("r2", "", "c1"), new Value("cX".getBytes()));

    {
      List<Class<? extends ApplyOp>> applyOps = new ArrayList<>(2);
      applyOps.add(AppendApply.class);
      applyOps.add(DuplicateApply.class);
      List<Map<String, String>> optionMaps = new ArrayList<>(2);
      optionMaps.add(Collections.singletonMap("str", str));
      optionMaps.add(Collections.<String, String>emptyMap());
      IteratorSetting multiApplyItset = MultiApply.iteratorSetting(1, applyOps, optionMaps);

      SortedKeyValueIterator<Key, Value> skvi = new MapIterator(input);
      skvi.init(null, null, null);
      SortedKeyValueIterator<Key, Value> skviTop = new ApplyIterator();
      skviTop.init(skvi, multiApplyItset.getOptions(), null);
      skvi = skviTop;
      skvi.seek(new Range(), Collections.<ByteSequence>emptySet(), false);

      IteratorAdapter ia = new IteratorAdapter(skvi);
      for (Map.Entry<Key, Value> expectEntry : expect.entrySet()) {
        Assert.assertTrue(ia.hasNext());
        Map.Entry<Key, Value> actualEntry = ia.next();
        Assert.assertEquals(expectEntry, actualEntry);
        Assert.assertTrue(ia.hasNext()); // now for the duplicate
        actualEntry = ia.next();
        Assert.assertEquals(expectEntry, actualEntry);
//      System.out.println("MATCH "+expectEntry);
      }
      Assert.assertFalse(ia.hasNext());
    }
    {
      List<Class<? extends ApplyOp>> applyOps = new ArrayList<>(2);
      applyOps.add(DuplicateApply.class);
      applyOps.add(AppendApply.class);
      List<Map<String, String>> optionMaps = new ArrayList<>(2);
      optionMaps.add(Collections.<String, String>emptyMap());
      optionMaps.add(Collections.singletonMap("str", str));
      IteratorSetting multiApplyItset = MultiApply.iteratorSetting(1, applyOps, optionMaps);

      SortedKeyValueIterator<Key, Value> skvi = new MapIterator(input);
      skvi.init(null, null, null);
      SortedKeyValueIterator<Key, Value> skviTop = new ApplyIterator();
      skviTop.init(skvi, multiApplyItset.getOptions(), null);
      skvi = skviTop;
      skvi.seek(new Range(), Collections.<ByteSequence>emptySet(), false);

      IteratorAdapter ia = new IteratorAdapter(skvi);
      for (Map.Entry<Key, Value> expectEntry : expect.entrySet()) {
        Assert.assertTrue(ia.hasNext());
        Map.Entry<Key, Value> actualEntry = ia.next();
        Assert.assertEquals(expectEntry, actualEntry);
        Assert.assertTrue(ia.hasNext()); // now for the duplicate
        actualEntry = ia.next();
        Assert.assertEquals(expectEntry, actualEntry);
      }
      Assert.assertFalse(ia.hasNext());
    }
  }

  @Test
  public void testTableConfigSerializes() {
    TableConfig tcOrig = new TableConfig(
        ClientConfiguration.loadDefault().withInstance("some_instance"),
        "tablename", "user", new PasswordToken("bla"));
    String str = SerializationUtil.serializeBase64(tcOrig);
    TableConfig tcAfter = (TableConfig)SerializationUtil.deserializeBase64(str);
    Assert.assertEquals(tcOrig, tcAfter);
  }

  @Test
  public void testInputTableConfigSerializes() {
    InputTableConfig tcOrig = new TableConfig(
        ClientConfiguration.loadDefault().withInstance("some_instance"),
        "tablename", "user", new PasswordToken("bla")).asInput().withColFilter("bla,");
    String str = SerializationUtil.serializeBase64(tcOrig);
    InputTableConfig tcAfter = (InputTableConfig)SerializationUtil.deserializeBase64(str);
    Assert.assertEquals(tcOrig, tcAfter);
  }

  @Test
  public void testOutputTableConfigSerializes() {
    OutputTableConfig tcOrig = new TableConfig(
        ClientConfiguration.loadDefault().withInstance("some_instance"),
        "tablename", "user", new PasswordToken("bla")).asOutput().withApplyLocal(DuplicateApply.class, null);
    String str = SerializationUtil.serializeBase64(tcOrig);
    OutputTableConfig tcAfter = (OutputTableConfig)SerializationUtil.deserializeBase64(str);
    Assert.assertEquals(tcOrig, tcAfter);
  }

  @Test
  public void testTableConfigCopyConstructor() {
    TableConfig tcOrig = new TableConfig(
        ClientConfiguration.loadDefault().withInstance("some_instance"),
        "tablename", "user", new PasswordToken("bla"));
    TableConfig tcClone = tcOrig.clone();
    TableConfig tcCopy = new TableConfig(tcOrig);
    Assert.assertEquals(tcClone, tcCopy);
    Assert.assertEquals(tcClone.hashCode(), tcCopy.hashCode());
  }

  @Test
  public void testInputTableConfigCopyConstructor() {
    InputTableConfig tcOrig = new TableConfig(
        ClientConfiguration.loadDefault().withInstance("some_instance"),
        "tablename", "user", new PasswordToken("bla")).asInput()
        .withColFilter(Collections.singleton(new Range("a", "b")));
    InputTableConfig tcClone = tcOrig.clone();
    InputTableConfig tcCopy = new InputTableConfig(tcOrig);
    Assert.assertEquals(tcClone, tcCopy);
    Assert.assertEquals(tcClone.hashCode(), tcCopy.hashCode());
  }

  @Test
  public void testOutputTableConfigCopyConstructor() {
    OutputTableConfig tcOrig = new TableConfig(
        ClientConfiguration.loadDefault().withInstance("some_instance"),
        "tablename", "user", new PasswordToken("bla")).asOutput()
        .withTableItersRemote(DynamicIteratorSetting.of(new IteratorSetting(2, DevNull.class)));
    OutputTableConfig tcClone = tcOrig.clone();
    OutputTableConfig tcCopy = new OutputTableConfig(tcOrig);
    Assert.assertEquals(tcClone, tcCopy);
    Assert.assertEquals(tcClone.hashCode(), tcCopy.hashCode());
  }

  private volatile TableConfig GlobalTableConfig;

  @Test
  public void testTableConfigMultithreadVisibility() throws InterruptedException {
    GlobalTableConfig = new TableConfig(
        ClientConfiguration.loadDefault().withInstance("some_instance"),
        "tablename", "user", new PasswordToken("bla"));

    Thread thread = new Thread() {
      @Override
      public void run() {
        GlobalTableConfig = GlobalTableConfig.withTableName("NewName");
        Assert.assertEquals("NewName", GlobalTableConfig.getTableName());
      }
    };
    long ts = System.currentTimeMillis();
    thread.run();
    while (!GlobalTableConfig.getTableName().equals("NewName")) {
      if (System.currentTimeMillis() - ts > 10000)
        Assert.fail("main thread did not see update to GlobalTableConfig");
    }
    thread.join();
  }

  @Test
  public void testRangeUnion() {
    SortedSet<Range> ranges = new TreeSet<>();
    ranges.add(new Range("a"));
    ranges.add(new Range("g"));
    ranges.add(new Range("x5", "y4"));
    ranges.add(new Range("k"));
    Range union = GraphuloUtil.unionAll(ranges);
    Assert.assertEquals(new Range("a", "y4"), union);
  }

}
