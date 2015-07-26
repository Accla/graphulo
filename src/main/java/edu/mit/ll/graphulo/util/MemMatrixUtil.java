package edu.mit.ll.graphulo.util;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.math3.linear.DefaultRealMatrixChangingVisitor;
import org.apache.commons.math3.linear.DefaultRealMatrixPreservingVisitor;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Invert a matrix.
 */
public class MemMatrixUtil {
  private static final Logger log = LogManager.getLogger(MemMatrixUtil.class);

  public static RealMatrix doInverse(RealMatrix matrix) {
    return new LUDecomposition(matrix).getSolver().getInverse();
  }

  public static RealMatrix buildMatrix(Iterator<Map.Entry<Key, Value>> iter, int dimension) {
    RealMatrix matrix = MatrixUtils.createRealMatrix(dimension, dimension);
    Text row = new Text(), col = new Text();
    while (iter.hasNext()) {
      Map.Entry<Key, Value> entry = iter.next();
      Key k = entry.getKey();
      int r = Integer.parseInt(k.getRow(row).toString()) - 1,
          c = Integer.parseInt(k.getColumnQualifier(col).toString()) - 1;
      double v = Double.parseDouble(new String(entry.getValue().get())); // StandardCharsets.UTF_8?
      matrix.setEntry(r, c, v);
    }
    return matrix;
  }

  private static final Text EMPTY_TEXT = new Text();

  public static SortedMap<Key,Value> matrixToMap(final SortedMap<Key,Value> map, RealMatrix matrix) {
    matrix.walkInOptimizedOrder(new DefaultRealMatrixPreservingVisitor() {
      private Text trow = new Text(), tcol = new Text();
      @Override
      public void visit(int row, int column, double v) {
//        log.debug("("+row+","+column+") <- "+v);
        trow.set(Integer.toString(row + 1).getBytes());
        tcol.set(Integer.toString(column + 1).getBytes());
        map.put(new Key(trow, EMPTY_TEXT, tcol, System.currentTimeMillis()),
            new Value(Double.toString(v).getBytes()));
      }
    });
//    log.debug("map "+map);
    return map;
  }

  /** Replace row and col labels with integer indexes; create map from indexes to original labels */
  public static RealMatrix indexMapAndMatrix(final Map<Key,Value> orig, final SortedMap<Integer,String> rowMap, final SortedMap<Integer,String> colMap) {
    // use Map<Integer,Map<Integer,Double>> to build
    SortedMap<Integer,SortedMap<Integer,Double>> rowcolvalmap = new TreeMap<>();
    Map<String,Integer> rowMapRev = new HashMap<>(), colMapRev = new HashMap<>();

    Text rowText = new Text(), colText = new Text();
    for (Map.Entry<Key, Value> entry : orig.entrySet()) {
      Key k = entry.getKey();
      String row = k.getRow(rowText).toString(), col = k.getColumnQualifier(colText).toString();
      Integer rowInt = rowMapRev.get(row);
      SortedMap<Integer,Double> colvalmap;
      if (rowInt == null) {
        rowInt = rowMapRev.size()+1;
        rowMapRev.put(row, rowInt);
        rowMap.put(rowInt, row);
        colvalmap = new TreeMap<>();
        rowcolvalmap.put(rowInt, colvalmap);
      } else
        colvalmap = rowcolvalmap.get(rowInt);
      Integer colInt = colMapRev.get(col);
      if (colInt == null) {
        colInt = colMapRev.size()+1;
        colMapRev.put(col, colInt);
        colMap.put(colInt, col);
      }
      colvalmap.put(colInt, Double.valueOf(new String(entry.getValue().get())));

    }

    // build matrix
    int N = rowMap.size();
    int M = colMap.size();
    RealMatrix m = MatrixUtils.createRealMatrix(N, M);
    for (Map.Entry<Integer, SortedMap<Integer, Double>> rcentry : rowcolvalmap.entrySet()) {
      int rowInt = rcentry.getKey();
      for (Map.Entry<Integer, Double> centry : rcentry.getValue().entrySet()) {
        int colInt = centry.getKey();
        double val = centry.getValue();
        m.setEntry(rowInt-1, colInt-1, val);
      }
    }
    return m;
  }

  public static Map<Key, Value> matrixToMapWithLabels(RealMatrix orig, final SortedMap<Integer, String> labelMap, final boolean labelColQ) {
    final Map<Key,Value> ret = new TreeMap<>();

    orig.walkInOptimizedOrder(new DefaultRealMatrixPreservingVisitor() {
      @Override
      public void visit(int row, int column, double value) {
        row++; column++;
        Text rowText, cqText;
        // labelColQ==false ==> rowText is label looked up, cq is integer as string
        if (!labelColQ) {
          rowText = new Text(labelMap.get(row));
          cqText = new Text(Integer.toString(column));
        } else {
          rowText = new Text(Integer.toString(row));
          cqText = new Text(labelMap.get(column));
        }
        Key k = new Key(rowText, EMPTY_TEXT, cqText);
        Value v = new Value(Double.toString(value).getBytes());
        ret.put(k,v);
      }
    });

    return ret;
  }

  public static RealMatrix randNormPosFull(int N, int K) {
    RealMatrix m = MatrixUtils.createRealMatrix(N,K);
    m.walkInOptimizedOrder(new DefaultRealMatrixChangingVisitor() {
      Random rand = new Random();
      @Override
      public double visit(int row, int column, double value) {
        return Math.abs(rand.nextGaussian());
      }
    });
    return m;
  }
}
