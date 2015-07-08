package edu.mit.ll.graphulo.skvi;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Matrix inverse. Values interpreted as doubles, encoded as Strings.
 * Designed to be used at full major compaction so that it sees all the entries.
 */
public class InverseMatrixIterator implements SortedKeyValueIterator<Key,Value> {
  private static final Logger log = LogManager.getLogger(MapIterator.class);

  private SortedKeyValueIterator<Key,Value> source, mapIterator;
//  private enum STATE {NEW, INITED, SEEKED }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    return new InverseMatrixIterator();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    if (!range.isInfiniteStartKey() || !range.isInfiniteStopKey())
      log.warn("Range is not infinite: "+range);
    source.seek(range, columnFamilies, inclusive);

    SortedMap<Key, Value> mapEntries = new TreeMap<>();
    int numRows = 1, numCols = 1;
    Text row = new Text(), col = new Text();
    while (source.hasTop()) {
      Key k = source.getTopKey();
      int r = Integer.parseInt(k.getRow(row).toString()),
          c = Integer.parseInt(k.getColumnQualifier(col).toString());
      numRows = Math.max(numRows, r);
      numCols = Math.max(numCols, c);
      mapEntries.put(new Key(k), source.getTopValue());
      source.next();
    }

    mapEntries = doInverse(mapEntries, numRows, numCols);

    mapIterator = new MapIterator(mapEntries);
    mapIterator.init(null, null, null);
    mapIterator.seek(range, columnFamilies, inclusive);
  }

  @Override
  public Key getTopKey() {
    return mapIterator.getTopKey();
  }

  @Override
  public Value getTopValue() {
    return mapIterator.getTopValue();
  }

  @Override
  public boolean hasTop() {
    return mapIterator.hasTop();
  }

  @Override
  public void next() throws IOException {
    mapIterator.next();
  }


  private static SortedMap<Key, Value> doInverse(SortedMap<Key, Value> mapEntries, int numRows, int numCols) {
    System.out.println("before inverse");
    for (Map.Entry<Key, Value> entry : mapEntries.entrySet()) {
      System.out.println(entry.getKey().toStringNoTime() + " -> " + entry.getValue());
    }
    double[][] arr = entriesToDoubles(mapEntries, numRows, numCols); // todo inefficient; better to construct matrix incrementally
    System.out.println(Arrays.deepToString(arr));

    RealMatrix matrix = MatrixUtils.createRealMatrix(arr);
    matrix = new LUDecomposition(matrix).getSolver().getInverse();
    arr = matrix.getData(); // todo inefficient; excessive copying of data. Make map from the matrix directly using a walker

    System.out.println("after  inverse");
    System.out.println(Arrays.deepToString(arr));
    mapEntries = doublesToEntries(arr);
    return mapEntries;
  }

  private static double[][] entriesToDoubles(SortedMap<Key, Value> mapEntries, int numRows, int numCols) {
    double[][] arr = new double[numRows][numCols];
    Text row = new Text(), col = new Text();
    for (Map.Entry<Key, Value> entry : mapEntries.entrySet()) {
      Key k = entry.getKey();
      int r = Integer.parseInt(k.getRow(row).toString()) - 1,
          c = Integer.parseInt(k.getColumnQualifier(col).toString()) - 1;
      double v = Double.parseDouble(new String(entry.getValue().get(), StandardCharsets.UTF_8));
      arr[r][c] = v;
    }
    return arr;
  }

  private static final Text EMPTY_TEXT = new Text();

  private static SortedMap<Key, Value> doublesToEntries(double[][] arr) {
    SortedMap<Key,Value> map = new TreeMap<>();
    Text trow = new Text(), tcol = new Text();
    for (int nrow = 0; nrow < arr.length; nrow++) {
      double[] drow = arr[nrow];
      trow.set(Integer.toString(nrow + 1).getBytes());
      for (int ncol = 0; ncol < drow.length; ncol++) {
        double v = drow[ncol];
        tcol.set(Integer.toString(ncol + 1).getBytes());
        map.put(new Key(trow, EMPTY_TEXT, tcol, System.currentTimeMillis()),
            new Value(Double.toString(v).getBytes()));
      }
    }
    return map;
  }

}
