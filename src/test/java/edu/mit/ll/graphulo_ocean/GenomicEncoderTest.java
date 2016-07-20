package edu.mit.ll.graphulo_ocean;

import org.junit.Assert;
import org.junit.Test;

/**
 *
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

}