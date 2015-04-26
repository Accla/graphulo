package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.Combiner;

import java.util.Collection;

/**
 * Graphulo interface
 */
public interface IGraphulo {

  /**
   * C += A * B.
   * User-defined "plus" and "multiply". Requires transpose table AT instead of A.
   * If C is not given, then the scan itself returns the results of A * B.
   * After operation, flushes C and removes the "plus" combiner from C.
   *
   * @param ATtable     Name of Accumulo table holding matrix transpose(A).
   * @param Btable      Name of Accumulo table holding matrix B.
   * @param Ctable      Optional. Name of table to store result. Streams back result if null.
   * @param multOp      An operation that "multiplies" two values.
   * @param sumOp       An operation that "sums" values.
   * @param rowFilter   Optional. Row subset of ATtable and Btable, like "a,:,b,g,c,:,"
   * @param colFilterAT Optional. Column qualifier subset of AT, restricted to not allow ranges.
   * @param colFilterB  Optional. Column qualifier subset of B, like "a,f,b,c,"
   */
  void TableMult(String ATtable, String Btable, String Ctable,
                 Class<? extends IMultiplyOp> multOp, Class<? extends Combiner> sumOp,
                 Collection<Range> rowFilter,
                 String colFilterAT, String colFilterB);

  /**
   * Create copy and/or transpose of original table, taking rowFilter and colFilter as a subset.
   *
   * @param table         Name of existing table.
   * @param tableCopy     Optional. Name of table to write a copy of the original.
   * @param tableTranspose Optional. Name of table to write transpose to. Created if not existing.
   * @param rowFilter     Row subset of original table.
   * @param colFilter     Column qualifier subset of original table, restricted to not allow ranges.
   * @param minDegree     Optional. Minimum out-degree.
   * @param maxDegree     Optional. Maximum out-degree.
   * @param gatherColQs Stream back a set of the unique columns found in the original table.
   */
  void TableCopyFilter(String table, String tableCopy, String tableTranspose,
                 Collection<Range> rowFilter, String colFilter,
                 int minDegree, int maxDegree, boolean gatherColQs);

  // Create k0 table even if it is empty (e.g. v0="")
  String AdjBFS(String Atable, String v0, int k, String Rtable, String RtableTranspose,
                String ADegtable, String degColumn, boolean degInColQ, int minDegree, int maxDegree);

  /**
   * Out-degree-filtered Breadth First Search on Incidence table.
   * Conceptually k iterations of: v0 ==startPrefix==> edge ==endPrefix==> v1.
   *
   * @param Etable         Incidence table; rows are edges, column qualifiers are nodes.
   * @param v0             Starting vertices, like "v0,v5,v6,"
   * @param k              # of hops.
   * @param startPrefix    Prefix of 'start' of an edge including separator, e.g. 'out|'
   * @param endPrefix      Prefix of 'end' of an edge including separator, e.g. 'in|'
   * @param minDegree      Optional. Minimum out-degree.
   * @param maxDegree      Optional. Maximum out-degree.
   * @param outputNormal   Create E of subgraph for each of the k hops.
   * @param outputTranpose Create ET of subgraph for each of the k hops.
   */
  void EdgeBFS(String Etable, String v0, int k,
               String startPrefix, String endPrefix,
               int minDegree, int maxDegree,
               boolean outputNormal, boolean outputTranpose);
  // String EtableDeg,

  void SingleTableBFS(String Stable, String v0, int k,
                      int minDegree, int maxDegree,
                      boolean outputNormal, boolean outputTranspose);

}
