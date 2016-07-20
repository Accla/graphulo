package edu.mit.ll.graphulo_ocean;

import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.iterators.ValueFormatException;

import java.util.Arrays;

/**
 *
 */
public class GenomicEncoder implements Lexicoder<char[]> {

  /** Length of k-mer. */
  private final int K;
  /** Number of bytes required to encode the k-mer. */
  private final int NB;
  /** Remainder - number of bases in the final byte. */
  private final int REM;

  public GenomicEncoder(int k) {
    K = k;
    NB = (K-1) / 4 + 1;
    REM = K == 0 ? 0 : (K-1) % 4 + 1;
  }

  /** Single-character encoding; primitive for enc(char[]). */
  private static byte enc(char b) {
    switch (b) {
      case 'A': return 0b00;
      case 'T': return 0b11;
      case 'C': return 0b01;
      case 'G': return 0b10;
      default: throw new IllegalArgumentException("bad base: "+b);
    }
  }

  private static byte enc(char[] bs) {
    return enc(bs, 0, bs.length);
  }

  /** Encode up to 4 characters in a byte */
  private static byte enc(char[] bs, int off, int len) {
//    assert off >= 0 && bs.length <= 4 : "bad length of bs";
    byte r = 0;
    for (int i = 0; i < len; i++) {
      int p = 3-i;
      r |= enc(bs[off+i]) << 2*p;
    }
    return r;
  }

  @Override
  public byte[] encode(char[] bs) {
    return encode(bs, 0);
  }

  public byte[] encode(char[] bs, int off) {
    if (off > bs.length-K)
      throw new IllegalArgumentException("input does not match length K="+K+": "+new String(bs)+" and off = "+off);
    byte[] ret = new byte[NB];
    for (int i = 0; i < NB; i++) {
      ret[i] = enc(bs, off + 4*i, i == NB-1 ? REM : 4);
    }
    return ret;
  }

  /** Copy sig characters into output starting at index, decoding from prefix of b. */
  private static void dec(byte b, int sig, char[] output, int index) {
    for (int i = 0; i < sig; i++) {
      int p = 3-i;
      byte bpart = (byte)((b >>> 2*p) & 0b11);
      char o;
      switch (bpart) {
        case 0b00: o = 'A'; break;
        case 0b11: o = 'T'; break;
        case 0b01: o = 'C'; break;
        case 0b10: o = 'G'; break;
        default: throw new IllegalArgumentException("bad base: "+bpart);
      }
      output[i+index] = o;
    }
  }

  @Override
  public char[] decode(byte[] b) throws ValueFormatException {
    if (b.length != NB)
      throw new IllegalArgumentException("input does not match length NB="+NB+": "+ Arrays.toString(b));
    char[] ret = new char[K];
    for (int i = 0; i < NB; i++) {
      dec(b[i], i == NB - 1 ? REM : 4, ret, 4*i);
    }
    return ret;
  }

//  public void reverseComplement(byte[] bs) {
//    if (bs.length != NB)
//      throw new IllegalArgumentException("input does not match length NB="+NB+": "+ Arrays.toString(b));
//    byte[] ret = new byte[NB];
//    for (int j = NB-1; )
//    for (int i = 0, j = NB-1; i <= j; i++, j--) {
//      byte btmp = bs[i];
//      bs[i] = reverseComplement(bs[j], j)
//    }
//    return ret;
//  }
//
//  /** Reverse and complement the first sig bits of x */
//  private static byte reverseComplement(byte x, int sig) {
////    byte b = 0;
////    for (int i = 2*(4-sig), j = 7; i < 8; i++, j--) {
////      b |=
////    }
//    x >>>= 2*(4-sig);
//    byte b = 0;
//    while (x != 0) {
//      b <<= 1;
//      b |= (~x) & 1;
//      x >>= 1;
//    }
//    return b;
//  }
}
