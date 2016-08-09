package edu.mit.ll.graphulo_ocean;

import edu.mit.ll.graphulo.DynamicIteratorSetting;
import edu.mit.ll.graphulo.skvi.D4mRangeFilter;
import edu.mit.ll.graphulo.skvi.DynamicIterator;
import edu.mit.ll.graphulo.skvi.MapIterator;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mock.IteratorAdapter;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static edu.mit.ll.graphulo.UtilTest.mockIteratorEnvrironment;
import static edu.mit.ll.graphulo.util.GraphuloUtil.EMPTY_BYTES;
import static edu.mit.ll.graphulo_ocean.GenomicEncoder.bytesToInt;
import static edu.mit.ll.graphulo_ocean.GenomicEncoder.reverseComplement;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test encoding, decoding of k-mer character arrays to/from byte arrays.
 * Test reverse complement on encoded form.
 */
public class GenomicEncoderTest {
  @Test
  public void encode() throws Exception {
    Assert.assertArrayEquals(new byte[]{0b00011110}, new GenomicEncoder(4).encode(new char[]{'A','C','T','G'}));
    Assert.assertArrayEquals(new byte[]{0b00011110}, new GenomicEncoder(4).encode(new char[]{'A','A','C','T','G'}, 1));
    Assert.assertArrayEquals(new byte[]{0b00011100}, new GenomicEncoder(3).encode(new char[]{'A','C','T'}));
    Assert.assertArrayEquals(new byte[]{0b00011110,(byte)0b11000000}, new GenomicEncoder(5).encode(new char[]{'A','C','T','G','T'}));
  }

  @Test
  public void decode() throws Exception {
    Assert.assertArrayEquals(new char[]{'A','C','T','G'}, new GenomicEncoder(4).decode(new byte[]{0b00011110}));
    Assert.assertArrayEquals(new char[]{'A','C','T'}, new GenomicEncoder(3).decode(new byte[]{0b00011100}));
    Assert.assertArrayEquals(new char[]{'A','C','T','G','T'}, new GenomicEncoder(5).decode(new byte[]{0b00011110,(byte)0b11000000}));
  }

  @Test
  public void decodeEncode() {
    GenomicEncoder g;
    char[] c;
    g = new GenomicEncoder(1); c = new char[]{'G'};
    Assert.assertArrayEquals(c, g.decode(g.encode(c)));
    Assert.assertArrayEquals(c, g.decode(g.intToBytes(bytesToInt(g.encode(c)))));
    g = new GenomicEncoder(4); c = new char[]{'A','C','T','G'};
    Assert.assertArrayEquals(c, g.decode(g.encode(c)));
    Assert.assertArrayEquals(c, g.decode(g.intToBytes(bytesToInt(g.encode(c)))));
    g = new GenomicEncoder(3); c = new char[]{'A','C','T'};
    Assert.assertArrayEquals(c, g.decode(g.encode(c)));
    Assert.assertArrayEquals(c, g.decode(g.intToBytes(bytesToInt(g.encode(c)))));
    g = new GenomicEncoder(5); c = new char[]{'A','C','T','G','T'};
    Assert.assertArrayEquals(c, g.decode(g.encode(c)));
    Assert.assertArrayEquals(c, g.decode(g.intToBytes(bytesToInt(g.encode(c)))));
    g = new GenomicEncoder(11); c = "AAAGTAACACA".toCharArray();
    Assert.assertArrayEquals(c, g.decode(g.encode(c)));
    Assert.assertArrayEquals(c, g.decode(g.intToBytes(bytesToInt(g.encode(c)))));
  }

  @Test
  public void generateLookupTable() {

  }

  @Test
  public void testReverseComplement1() {
    GenomicEncoder g4 = new GenomicEncoder(4), g3 = new GenomicEncoder(3);
    byte[][] tests = new byte[][] {
      new byte[] { g4.encode(new char[] {'A','C','T','G'})[0], reverseComplement(g4.encode(new char[] {'C','A','G','T'})[0],4) },
      new byte[] { g4.encode(new char[] {'A','A','A','A'})[0], reverseComplement(g4.encode(new char[] {'T','T','T','T'})[0],4) },
      new byte[] { g4.encode(new char[] {'C','C','C','C'})[0], reverseComplement(g4.encode(new char[] {'G','G','G','G'})[0],4) },
      new byte[] { g4.encode(new char[] {'T','C','C','C'})[0], reverseComplement(g4.encode(new char[] {'G','G','G','A'})[0],4) },
      new byte[] { g3.encode(new char[] {'T','C','C'})[0], reverseComplement(g3.encode(new char[] {'G','G','A'})[0],3) },
      new byte[] { g3.encode(new char[] {'T','C','G'})[0], reverseComplement(g3.encode(new char[] {'C','G','A'})[0],3) }
    };
    for (int i = 0; i < tests.length; i++) {
      byte[] test = tests[i];
      Assert.assertEquals("test "+i, test[0], test[1]);
    }
  }

  @Test
  public void testReverseComplementFull() {
    GenomicEncoder g4 = new GenomicEncoder(4), g3 = new GenomicEncoder(3), g5 = new GenomicEncoder(5), g7 = new GenomicEncoder(7), g11 = new GenomicEncoder(11);
    byte[][][] tests = new byte[][][] {
        new byte[][] { g4.encode(new char[] {'A','C','T','G'}), g4.reverseComplement(g4.encode(new char[] {'C','A','G','T'})) },
        new byte[][] { g4.encode(new char[] {'A','A','A','A'}), g4.reverseComplement(g4.encode(new char[] {'T','T','T','T'})) },
        new byte[][] { g4.encode(new char[] {'C','C','C','C'}), g4.reverseComplement(g4.encode(new char[] {'G','G','G','G'})) },
        new byte[][] { g4.encode(new char[] {'T','C','C','C'}), g4.reverseComplement(g4.encode(new char[] {'G','G','G','A'})) },
        new byte[][] { g3.encode(new char[] {'T','C','C'}), g3.reverseComplement(g3.encode(new char[] {'G','G','A'})) },
        new byte[][] { g3.encode(new char[] {'T','C','G'}), g3.reverseComplement(g3.encode(new char[] {'C','G','A'})) },
        new byte[][] { g5.encode(new char[] {'A','A','C','T','G'}), g5.reverseComplement(g5.encode(new char[] {'C','A','G','T','T'})) },
        new byte[][] { g5.encode("ATCTG".toCharArray()), g5.reverseComplement(g5.encode("CAGAT45trret43t".toCharArray())) },
        new byte[][] { g7.encode(new char[] {'C','G','A','A','C','T','G'}), g7.reverseComplement(g7.encode(new char[] {'C','A','G','T','T','C','G'})) },
        new byte[][] { g11.encode("TTTTTTTTTTT".toCharArray()), g11.reverseComplement(g11.encode("AAAAAAAAAAA45trret43t".toCharArray())) },
        new byte[][] { g11.encode("ATCTGTTTTTT".toCharArray()), g11.reverseComplement(g11.encode("AAAAAACAGAT45trret43t".toCharArray())) }
    };
    for (int i = 0; i < tests.length; i++) {
      byte[][] test = tests[i];
      Assert.assertArrayEquals("test "+i, test[0], test[1]);
    }

  }

  @Test
  public void testReverseComplementIterator() throws Exception {
    IteratorSetting itset = new DynamicIteratorSetting(1, "hi")
        .append(ReverseComplementApply.iteratorSetting(1, D4mRangeFilter.KeyPart.COLQ, 5))
        .toIteratorSetting();
    Map<String,String> opts = itset.getOptions();

    GenomicEncoder g5 = new GenomicEncoder(5);
    SortedMap<Key,Value> input = new TreeMap<>();
    input.put(new Key("r1".getBytes(UTF_8), EMPTY_BYTES, g5.encode(new char[] {'A','A','C','T','G'}), EMPTY_BYTES, 1, false, false), new Value("1".getBytes(UTF_8)));

    SortedKeyValueIterator<Key,Value> skvi = new MapIterator(input);
    skvi.init(null, null, null);
    SortedKeyValueIterator<Key,Value> skviTop = new DynamicIterator();
    skviTop.init(skvi, opts, mockIteratorEnvrironment(DynamicIteratorSetting.MyIteratorScope.MINC));
    skvi = skviTop;
    skvi.seek(new Range(), Collections.<ByteSequence>emptySet(), false);

    SortedMap<Key,Value> expect = new TreeMap<>();
    expect.put(new Key("r1".getBytes(UTF_8), EMPTY_BYTES, g5.reverseComplement(g5.encode(new char[] {'A','A','C','T','G'})), EMPTY_BYTES, 1, false, false), new Value("1".getBytes(UTF_8)));

    IteratorAdapter ia = new IteratorAdapter(skvi);
    //    while (ia.hasNext()) {
//      Map.Entry<Key, Value> next = ia.next();
//      System.out.println(next.getKey().toString()+" -> "+next.getValue());
//    }
    for (Map.Entry<Key, Value> expectEntry : expect.entrySet()) {
      Assert.assertTrue(ia.hasNext());
      Map.Entry<Key, Value> actualEntry = ia.next();
      Assert.assertEquals(expectEntry, actualEntry);
    }
    Assert.assertFalse(ia.hasNext());

  }

}