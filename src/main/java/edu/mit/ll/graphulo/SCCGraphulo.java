package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class SCCGraphulo extends Graphulo {

  public SCCGraphulo(Connector connector, PasswordToken password) {
    super(connector, password);
  }

  /**
   * Adjacency Table Strongly Connected Components algorithm.
   * For directed graphs.
   * Result is stored in an Accumulo table.  The rows that belong to the same component have the same columns.
   * Ex:
   * <pre>
   *      1 0 1 0 0 0
   *      0 1 0 1 1 1
   *      1 0 1 0 0 0
   *      0 1 0 1 1 1
   *      0 1 0 1 1 1
   *      0 1 0 1 1 1
   * </pre>
   * 
   * is a result with two SCCs. The first and third node are in the same SCC, and the others are in another SCC.
   *
   * @param Atable
   *          Name of Accumulo table holding matrix A.
   * @param Rtable
   *          Name of table to store result.
   * @param rowCount
   *          Number of rows in the Atable. Will make optional in future updates.
   * @param trace
   *          Enable server-side performance tracing.
   */
  public void SCC(String Atable, String Rtable, long rowCount, /* boolean notSymmetric, */boolean trace) throws AccumuloSecurityException, AccumuloException,
      TableNotFoundException {
    if (Atable == null || Atable.isEmpty()) {
      throw new IllegalArgumentException("Please specify table A. Given: " + Atable);
    }
    if (Rtable == null || Rtable.isEmpty()) {
      throw new IllegalArgumentException("Please specify table AT. Given: " + Rtable);
    }
    if (rowCount < 1) {
      throw new IllegalArgumentException("Table too small.");
    }

    TableOperations tops = connector.tableOperations();
    String tA = Atable;
    String tAC = Atable + "_Copy";
    String tAT = Atable + "_Transpose";
    String tR = Rtable + "_Temporary";
    String tRT = Rtable + "_Transpose";
    String tRf = Rtable;

    // FOR TESTING PORPOISES
    Map<Key,Value> printer = new HashMap<>();

    AdjBFS(tA, null, 1, tR, tAT, null, -1, null, "", true, 0, Integer.MAX_VALUE, null);
    for (int k = 1; k < rowCount; k++) {
      if (k % 2 == 1) {
        if (k != 1)
          tops.delete(tAC);
        TableMult(tAT, tA, tR, tAC, -1, MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG, "", false), null, null, null, null, false, false, -1);

        if (trace) {
          // TESTING
          System.out.println("Writing to tAC:");
          Scanner scannertAC = connector.createScanner(tAC, Authorizations.EMPTY);
          scannertAC.setRange(new Range());
          for (Map.Entry<Key,Value> entry : scannertAC) {
            printer.put(entry.getKey(), entry.getValue());
          }
          for (Map.Entry<Key,Value> entry : printer.entrySet()) {
            System.out.println(entry.getKey() + " // " + entry.getValue());
          }
          scannertAC.close();
          printer.clear();
          // TEST END
        }

      } else {
        tops.delete(tAT);
        TableMult(tAC, tA, tR, tAT, -1, MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG, "", false), null, null, null, null, false, false, -1);

        if (trace) {
          // TESTING
          System.out.println("Writing to tAT:");
          Scanner scannertAT = connector.createScanner(tAT, Authorizations.EMPTY);
          scannertAT.setRange(new Range());
          for (Map.Entry<Key,Value> entry : scannertAT) {
            printer.put(entry.getKey(), entry.getValue());
          }
          for (Map.Entry<Key,Value> entry : printer.entrySet()) {
            System.out.println(entry.getKey() + " // " + entry.getValue());
          }
          scannertAT.close();
          printer.clear();
          // TEST END
        }

      }
    }

    AdjBFS(tR, null, 1, null, tRT, null, -1, null, "", true, 0, Integer.MAX_VALUE, null);

    if (trace) {
      // TESTING
      System.out.println("Getting tR:");
      Scanner scannertR = connector.createScanner(tR, Authorizations.EMPTY);
      scannertR.setRange(new Range());
      for (Map.Entry<Key,Value> entry : scannertR) {
        printer.put(entry.getKey(), entry.getValue());
      }
      for (Map.Entry<Key,Value> entry : printer.entrySet()) {
        System.out.println(entry.getKey() + " // " + entry.getValue());
      }
      scannertR.close();
      printer.clear();

      System.out.println("Getting tRT:");
      Scanner scannertRT = connector.createScanner(tRT, Authorizations.EMPTY);
      scannertRT.setRange(new Range());
      for (Map.Entry<Key,Value> entry : scannertRT) {
        printer.put(entry.getKey(), entry.getValue());
      }
      for (Map.Entry<Key,Value> entry : printer.entrySet()) {
        System.out.println(entry.getKey() + " // " + entry.getValue());
      }
      scannertRT.close();
      printer.clear();
      // TEST END
    }

    SpEWiseX(tR, tRT, tRf, null, -1, MathTwoScalar.class, MathTwoScalar.optionMap(MathTwoScalar.ScalarOp.TIMES, MathTwoScalar.ScalarType.LONG, "", false), null, null, null, null, -1);

  }

  /**
   * Interprets information from SCC algorithm. Returns a Set of Strings that list the SCCs of a graph or a singleton Set of just one if queried by node. Ex:
   * 
   * <pre>
   * {&quot;v0,v2,v3,v4,v9,&quot;, &quot;v1,v5,v6,&quot;}
   * </pre>
   * 
   * is a result with two SCCs. The nodes v0,v2,v3,v4,v9 are in one SCC, v1,v5,v6 in another, and v7 and v8 are in none.
   *
   * @param tA
   *          Name of Accumulo table holding SCC matrix A.
   * @param vertex
   *          Name of vertex whose SCC you want. Null means it will return all SCCs.
   * @return Set of strongly connected components.
   */
  public Set<String> SCCQuery(String tA, String vertex) throws TableNotFoundException {
    if (vertex == null) {
      StringBuilder midstr = new StringBuilder();
      Set<String> vertset = new HashSet<>();
      Scanner scanner = connector.createScanner(tA, Authorizations.EMPTY);
      Text tmp = new Text();
      for (Map.Entry<Key,Value> entry : scanner) {
        midstr.append(entry.getKey().getRow(tmp).toString()).append(" ");
      }
      String delims = "[ ]+";
      String[] mid = midstr.toString().split(delims);
      for (String s : mid) {
        String bfs = AdjBFS(tA, s + ",", 1, null, null, null, -1, null, "", true, 0, Integer.MAX_VALUE, Graphulo.PLUS_ITERATOR_BIGDECIMAL);
        if (!bfs.isEmpty())
          vertset.add(bfs);
      }
      return vertset;
    }

    Set<String> vertset = new HashSet<>();
    String bfs = AdjBFS(tA, vertex, 1, null, null, null, -1, null, "", true, 0, Integer.MAX_VALUE, Graphulo.PLUS_ITERATOR_BIGDECIMAL);
    if (!bfs.isEmpty())
      vertset.add(bfs);
    return vertset;
  }
}
