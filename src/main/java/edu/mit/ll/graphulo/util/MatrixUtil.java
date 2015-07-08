package edu.mit.ll.graphulo.util;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealMatrixPreservingVisitor;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;

/**
 * Invert a matrix.
 */
public class MatrixUtil {
  private static final Logger log = LogManager.getLogger(MatrixUtil.class);

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
    matrix.walkInOptimizedOrder(new RealMatrixPreservingVisitor() {
      private Text trow = new Text(), tcol = new Text();
      @Override
      public void start(int rows, int columns, int startRow, int endRow, int startColumn, int endColumn) {
      }

      @Override
      public void visit(int row, int column, double v) {
        log.debug("("+row+","+column+") <- "+v);
        trow.set(Integer.toString(row + 1).getBytes());
        tcol.set(Integer.toString(column + 1).getBytes());
        map.put(new Key(trow, EMPTY_TEXT, tcol, System.currentTimeMillis()),
            new Value(Double.toString(v).getBytes()));
      }

      @Override
      public double end() {
        return 0;
      }
    });
    log.debug("map "+map);
    return map;
  }


}
