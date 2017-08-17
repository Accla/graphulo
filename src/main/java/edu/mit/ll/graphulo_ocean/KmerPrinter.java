package edu.mit.ll.graphulo_ocean;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Prints the GC content of all kmers to a file.
 */
public class KmerPrinter {

  private final int K;
  private final char[][] GC_DEC;

  public KmerPrinter(int k) {
    K = k;
    GC_DEC = new char[K+1][];
    for (int i = 0; i < K + 1; i++) {
      GC_DEC[i] = Double.toString(((double)i)/K).toCharArray();
    }
  }

  public void writeAllKmersAndGCContent(PrintWriter out) {

    out.write("kmer,gc_content\n");
    char[] mer = new char[K+1];
//    Arrays.fill(mer, 'A');
    mer[K] = ',';

    recurse(out, mer, 0, 0);
  }

  private void recurse(PrintWriter out, char[] mer, int pos, int gc) {
    if (pos == K) {
      out.write(mer);
      out.write(GC_DEC[gc]);
      out.append('\n');
    } else {
      // loop over the chars at this position
      mer[pos] = 'A';
      recurse(out, mer, pos+1, gc);
      mer[pos] = 'T';
      recurse(out, mer, pos+1, gc);
      mer[pos] = 'C';
      recurse(out, mer, pos+1, gc+1);
      mer[pos] = 'G';
      recurse(out, mer, pos+1, gc+1);
    }
  }

  public static void main(String[] args) throws IOException {
    File f = new File("kmer_gc.csv");
    KmerPrinter kgen = new KmerPrinter(11);
    try (PrintWriter fw = new PrintWriter(new BufferedWriter(new FileWriter(f)))) {
      kgen.writeAllKmersAndGCContent(fw);
    }
  }

}
