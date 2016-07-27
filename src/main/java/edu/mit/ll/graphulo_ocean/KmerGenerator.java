package edu.mit.ll.graphulo_ocean;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

/**
 * Generate a random sequence of given length, and write its kmer frequencies to a file.
 * Option to normalize, and to set the probability of G or C vs. A or T.
 */
public class KmerGenerator {

  private final int K;
  private final char[][] GC_DEC;

  public KmerGenerator(int k) {
    K = k;
    GC_DEC = new char[K+1][];
    for (int i = 0; i < K + 1; i++) {
      GC_DEC[i] = Double.toString(((double)i)/K).toCharArray();
    }
  }

  public static void main(String[] args) throws IOException {
    File f = new File("kmer_random_norm.csv");
    KmerGenerator kgen = new KmerGenerator(11);
    try (PrintWriter fw = new PrintWriter(new BufferedWriter(new FileWriter(f)))) {
      kgen.generateKmerCounts(fw, "Srand01", 963323535, true, 0.417);
    }
  }

  private final Random R = new Random(20160726);

  // 0, 1, 2, 3: G, C, A, T
  private byte genChar(double fix_gc) {
    if (R.nextDouble() < fix_gc)
      return (byte)R.nextInt(2);
    else
      return (byte)(R.nextInt(2) + 2);
  }

  private void decode(char[] b, int l) {
    for (int i = 0; i < K; i++) {
      switch(l & 0b11) {
        case 0: b[i] = 'G'; break;
        case 1: b[i] = 'C'; break;
        case 2: b[i] = 'T'; break;
        case 3: b[i] = 'A'; break;
        default: throw new AssertionError(l & 0b11);
      }
      l >>>= 2;
    }
  }

  private void generateKmerCounts(PrintWriter pw, String sampleid, int numkmers, boolean normalize, double fix_gc) {
    byte[] buf = new byte[10000];
    int[] countMap = new int[1 << (K << 1)];

    // generate first K-1 bases
    for (int i = 0; i < K - 1; i++) {
      buf[i] = genChar(fix_gc);
    }

    char[] mer = new char[K];

    int pos = K-1;
    for (int i = 0; i < numkmers; i++) {
      // check if we wrapped around
      if (pos == buf.length) {
        // copy K-1 bases to front
        System.arraycopy(buf, pos-K, buf, 0, K-1);
        pos = K-1;
      }

      // generate next base
      buf[pos] = genChar(fix_gc);

      // count kmer
      addKmer(buf, pos-K+1, countMap);

      pos++;
    }
    buf = null;

    // write out the map

    pw.println("sampleid,kmer," + (normalize ? "norm_cnt" : "cnt"));
    final String sidcomma = sampleid+",";
    for (int i = 0; i < countMap.length; i++) {
      if (countMap[i] != 0) {
        pw.print(sidcomma);
        decode(mer, i);
        pw.print(mer);
        pw.print(',');
        if (normalize)
          pw.println(((double)countMap[i])/numkmers);
        else
          pw.println(countMap[i]);
      }
    }
  }

//  char[] t = new char[11];
  private void addKmer(byte[] buf, int pos, int[] countMap) {
    int l = 0;
    for (int i = 0; i < K; i++) {
      l |= buf[pos + i] << (i << 1);
//      System.out.print(buf[pos+i]);
    }
//    decode(t, l);
//    System.out.println(" "+Arrays.toString(t));
    countMap[l]++;
  }

}
