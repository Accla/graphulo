package edu.mit.ll.graphulo_ocean;

import org.junit.Assert;
import org.junit.Test;

import static edu.mit.ll.graphulo_ocean.GenomicEncoder.reverseComplement;

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
    GenomicEncoder g = new GenomicEncoder(4);
    char[] c = new char[]{'A','C','T','G'};
    Assert.assertArrayEquals(c, g.decode(g.encode(c)));
    g = new GenomicEncoder(3); c = new char[]{'A','C','T'};
    Assert.assertArrayEquals(c, g.decode(g.encode(c)));
    g = new GenomicEncoder(5); c = new char[]{'A','C','T','G','T'};
    Assert.assertArrayEquals(c, g.decode(g.encode(c)));
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
    GenomicEncoder g4 = new GenomicEncoder(4), g3 = new GenomicEncoder(3), g5 = new GenomicEncoder(5), g7 = new GenomicEncoder(7);
    byte[][][] tests = new byte[][][] {
        new byte[][] { g4.encode(new char[] {'A','C','T','G'}), g4.reverseComplement(g4.encode(new char[] {'C','A','G','T'})) },
        new byte[][] { g4.encode(new char[] {'A','A','A','A'}), g4.reverseComplement(g4.encode(new char[] {'T','T','T','T'})) },
        new byte[][] { g4.encode(new char[] {'C','C','C','C'}), g4.reverseComplement(g4.encode(new char[] {'G','G','G','G'})) },
        new byte[][] { g4.encode(new char[] {'T','C','C','C'}), g4.reverseComplement(g4.encode(new char[] {'G','G','G','A'})) },
        new byte[][] { g3.encode(new char[] {'T','C','C'}), g3.reverseComplement(g3.encode(new char[] {'G','G','A'})) },
        new byte[][] { g3.encode(new char[] {'T','C','G'}), g3.reverseComplement(g3.encode(new char[] {'C','G','A'})) },
        new byte[][] { g5.encode(new char[] {'A','A','C','T','G'}), g5.reverseComplement(g5.encode(new char[] {'C','A','G','T','T'})) },
        new byte[][] { g5.encode(new char[] {'C','G','A','A','C','T','G'}), g5.reverseComplement(g5.encode(new char[] {'C','A','G','T','T','C','G'})) }
    };
  }

}