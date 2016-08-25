package edu.mit.ll.graphulo_ocean;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

/**
 * Encoding, decoding of k-mer character arrays to/from byte arrays.
 * Reverse complement on encoded form.
 */
public class GenomicEncoder implements Lexicoder<char[]> {

  /** Length of k-mer. */
  public final int K;
  /** Number of bytes required to encode the k-mer. */
  public final int NB;
  /** Remainder - number of bases in the final byte. A number from 1 to 4, unless K=0. */
  public final int REM;

  public GenomicEncoder(int k) {
    Preconditions.checkArgument(k > 0, "bad k ", k);
    K = k;
    NB = (K-1) / 4 + 1;
    REM = K == 0 ? 0 : (K-1) % 4 + 1;
  }

  /** Single-character encoding; primitive for enc1(char[]). */
  private static byte enc1(char b) {
    switch (b) {
      case 'A': return 0b00;
      case 'T': return 0b11;
      case 'C': return 0b01;
      case 'G': return 0b10;
      default: throw new IllegalArgumentException("bad base: "+b);
    }
  }

  private static byte enc1(char[] bs) {
    return enc1(bs, 0, bs.length);
  }

  /** Encode up to 4 characters in a byte */
  private static byte enc1(char[] bs, int off, int len) {
//    assert off >= 0 && len <= 4 : "bad length of bs";
    byte r = 0;
    for (int i = 0; i < len; i++)
      r |= enc1(bs[off + i]) << 2 * (3 - i);
    return r;
  }

  @Override
  public byte[] encode(char[] bs) {
    return encode(bs, 0);
  }

  /** Encode a k-mer of length k characters, starting from offset off of bs.  */
  public byte[] encode(char[] bs, int off) {
    return encode(bs, off, new byte[NB]);
  }

  /** Use an existing byte[] instead of allocating a new one. Ensure it is length at least NB. */
  public byte[] encode(char[] bs, int off, byte[] ret) {
    if (off > bs.length-K)
      throw new IllegalArgumentException("input does not match length K="+K+": "+new String(bs)+" and off = "+off);
    for (int i = 0; i < NB; i++)
      ret[i] = enc1(bs, off + 4 * i, i == NB - 1 ? REM : 4);
    return ret;
  }

  /** Copy sig characters into output starting at index, decoding from prefix of b. */
  private static void dec1(byte b, int sig, char[] output, int index) {
    for (int i = 0; i < sig; i++) {
      byte bpart = (byte)((b >>> 2* (3-i)) & 0b11);
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
  public char[] decode(byte[] b) {
    return decode(b, new char[K]);
  }

  /** Use an existing char[] instead of allocating a new one. Ensure it is length at least K. */
  public char[] decode(byte[] b, char[] ret) {
    if (b.length < NB)
      throw new IllegalArgumentException("input does not match length NB="+NB+": "+ Arrays.toString(b));
    for (int i = 0; i < NB; i++)
      dec1(b[i], i == NB - 1 ? REM : 4, ret, 4 * i);
    return ret;
  }

  /** Take the reverse complement of an encoded k-mer. Modifies the input bytes directly. */
  public byte[] reverseComplement(byte[] bs) {
    if (bs.length != NB)
      throw new IllegalArgumentException("input does not match length NB="+NB+": "+ Arrays.toString(bs));
    ArrayUtils.reverse(bs);
    for (int i = 0; i < NB; i++)
      bs[i] = reverseComplement(bs[i], i == 0 ? REM : 4);
    if (REM != 4) {
      byte orem642 = (byte) (0b11111111 >>> 2 * REM);
      byte lrem642 = (byte) (0b00111111 << 2*(4-REM) & 0b11111111);
      for (int i = 0; i < NB - 1; i++) {
        bs[i] |= bs[i+1] >>> 2 * REM & orem642;
        bs[i+1] = (byte) (bs[i+1] << 2*(4 - REM) & lrem642);
      }
    }
    return bs;
  }

  /** Reverse and complement the first sig bits of x */
  static byte reverseComplement(byte x, int sig) {
    byte nx = (byte)~x;
    return (byte) (((nx & 0b11000000) >>> 6
            | (nx & 0b00110000) >>> 2
            | (nx & 0b00001100) << 2
            | (nx & 0b00000011) << 6) << ((4-sig) << 1));
  }

  public static int bytesToInt(byte[] bs) {
    if (bs.length > 4)
      throw new IllegalArgumentException("too many bytes: "+bs.length);
    int res = 0;
    for (int i = 0; i < bs.length; i++) {
      byte b = bs[i];
      res |= (b & 0b11111111) << (24 - 8*i);
    }
    return res;
  }

  public byte[] intToBytes(int n) {
    return intToBytes(n, new byte[NB]);
  }

  public byte[] intToBytes(int n, byte[] res) {
    for (int i = 0; i < NB; i++)
      res[i] = (byte) (n >>> (24 - 8 * i) & 0b11111111);
    return res;
  }
}
