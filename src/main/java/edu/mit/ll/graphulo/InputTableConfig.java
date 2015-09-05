package edu.mit.ll.graphulo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.SortedSet;

/**
 * Immutable class representing a table used for input to an iterator stack via a local iterator or a RemoteSourceIterator.
 */
public final class InputTableConfig implements Serializable {

  public static final int DEFAULT_ITERS_REMOTE_PRIORITY = 50;
  static final Map<String,String> DEFAULT_ITERS_MAP =
      ImmutableMap.copyOf(new DynamicIteratorSetting(DEFAULT_ITERS_REMOTE_PRIORITY, null).buildSettingMap());
  private static final SortedSet<Range> ALL_RANGE = ImmutableSortedSet.of(new Range()); // please do not call the readFields() method of the range inside
  private static final String ALL_RANGE_STR = ":,";

  private static final long serialVersionUID = 1L;

  private final TableConfig tableConfig;
  private final Authorizations authorizations; // immutable and Serializable
  private final Map<String,String> itersRemote; // no null; copy on read, return ImmutableMap
  private final Map<String,String> itersClientSide;  // no need for special measures because DIS.buildSettingMap() and .fromMap() make new objects
                                                // combine the two when used as a local iterator as opposed to a RemoteSourceIterator
  private final String rowFilter, colFilter;    // no null, always store in sorted merged form, always keep aligned with the SortedSet<Range> versions
  private final transient SortedSet<Range> rowFilterRanges, colFilterRanges; // ^^

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    // small hack that enables setting a final variable
    try {
      Class<InputTableConfig> c = InputTableConfig.class;
      Field f;
      f = c.getDeclaredField("rowFilterRanges");
      f.setAccessible(true);
      f.set(this, GraphuloUtil.d4mRowToRanges(rowFilter)); // set to specific instance saved in class
      f = c.getDeclaredField("colFilterRanges");
      f.setAccessible(true);
      f.set(this, GraphuloUtil.d4mRowToRanges(colFilter));
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("impossible: fields rowFilterRanges and colFilterRanges exist", e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("trouble setting rowFilterRanges and colFilterRanges", e);
    }
  }

  public InputTableConfig(TableConfig tableConfig) {
    Preconditions.checkNotNull(tableConfig);
    this.tableConfig = tableConfig;
    authorizations = Authorizations.EMPTY;
    itersClientSide = itersRemote = DEFAULT_ITERS_MAP;
    rowFilter = ALL_RANGE_STR;
    colFilter = ALL_RANGE_STR;
    rowFilterRanges = colFilterRanges = ALL_RANGE;
  }

  private InputTableConfig(TableConfig tableConfig, Authorizations authorizations,
                           Map<String, String> itersRemote, Map<String, String> itersClientSide,
                           String rowFilter, String colFilter,
                           SortedSet<Range> rowFilterRanges, SortedSet<Range> colFilterRanges) {
    this.tableConfig = tableConfig;
    this.authorizations = authorizations;
    this.itersClientSide = itersClientSide;
    this.itersRemote = itersRemote;
    this.rowFilter = rowFilter;
    this.colFilter = colFilter;
    this.rowFilterRanges = rowFilterRanges;
    this.colFilterRanges = colFilterRanges;
  }

  public InputTableConfig withTableConfig(TableConfig tableConfig) {
    Preconditions.checkNotNull(tableConfig);
    return new InputTableConfig(tableConfig, authorizations, itersRemote, itersClientSide, rowFilter, colFilter, rowFilterRanges, colFilterRanges);
  }
  public InputTableConfig withAuthorizations(Authorizations authorizations) {
    if (authorizations == null) authorizations = Authorizations.EMPTY;
    return new InputTableConfig(tableConfig, authorizations, itersRemote, itersClientSide, rowFilter, colFilter, rowFilterRanges, colFilterRanges);
  }
  public InputTableConfig withItersRemote(DynamicIteratorSetting itersRemote) {
    return new InputTableConfig(tableConfig, authorizations, itersRemote == null ? DEFAULT_ITERS_MAP : itersRemote.buildSettingMap(), itersClientSide, rowFilter, colFilter, rowFilterRanges, colFilterRanges);
  }
  public InputTableConfig withItersLocal(DynamicIteratorSetting itersLocal) {
    return new InputTableConfig(tableConfig, authorizations, itersRemote, itersLocal == null ? DEFAULT_ITERS_MAP : itersLocal.buildSettingMap(), rowFilter, colFilter, rowFilterRanges, colFilterRanges);
  }
  public InputTableConfig withRowFilter(String rowFilter) {
    if (rowFilter == null || rowFilter.isEmpty())
      return new InputTableConfig(tableConfig, authorizations, itersRemote, itersClientSide, ALL_RANGE_STR, colFilter, ALL_RANGE, colFilterRanges);
    else {
      SortedSet<Range> set = ImmutableSortedSet.copyOf(Range.mergeOverlapping(GraphuloUtil.d4mRowToRanges(rowFilter)));
      rowFilter = GraphuloUtil.rangesToD4MString(set);
      return new InputTableConfig(tableConfig, authorizations, itersRemote, itersClientSide, rowFilter, colFilter, set, colFilterRanges);
    }
  }
  public InputTableConfig withColFilter(String colFilter) {
    if (colFilter == null || colFilter.isEmpty())
      return new InputTableConfig(tableConfig, authorizations, itersRemote, itersClientSide, rowFilter, ALL_RANGE_STR, rowFilterRanges, ALL_RANGE);
    else {
      SortedSet<Range> set = ImmutableSortedSet.copyOf(Range.mergeOverlapping(GraphuloUtil.d4mRowToRanges(colFilter)));
      colFilter = GraphuloUtil.rangesToD4MString(set);
      return new InputTableConfig(tableConfig, authorizations, itersRemote, itersClientSide, rowFilter, colFilter, set, colFilterRanges);
    }
  }
  public InputTableConfig withRowFilter(Collection<Range> rowFilterRanges) {
    Preconditions.checkArgument(rowFilterRanges != null && !rowFilterRanges.isEmpty());
    SortedSet<Range> rset = ImmutableSortedSet.copyOf(Range.mergeOverlapping(rowFilterRanges));
    return new InputTableConfig(tableConfig, authorizations, itersRemote, itersClientSide, GraphuloUtil.rangesToD4MString(rset), colFilter, rset, colFilterRanges);
  }
  public InputTableConfig withColFilter(Collection<Range> colFilterRanges) {
    Preconditions.checkArgument(colFilterRanges != null && !colFilterRanges.isEmpty());
    SortedSet<Range> rset = ImmutableSortedSet.copyOf(Range.mergeOverlapping(colFilterRanges));
    return new InputTableConfig(tableConfig, authorizations, itersRemote, itersClientSide, rowFilter, GraphuloUtil.rangesToD4MString(rset), rowFilterRanges, rset);
  }

  // less efficient way to do rowFilter and colFilter
//  public InputTableConfig withRowFilter(String rowFilter) {
//    DynamicIteratorSetting dis = DynamicIteratorSetting.fromMap(itersRemote);
//    dis.prepend(D4mRangeFilter.iteratorSetting(1, D4mRangeFilter.KeyPart.ROW, rowFilter));
//    return withItersRemote(dis);
//  }
//  public InputTableConfig withColFilter(String colFilter) {
//    DynamicIteratorSetting dis = DynamicIteratorSetting.fromMap(itersRemote);
//    dis.prepend(D4mRangeFilter.iteratorSetting(1, D4mRangeFilter.KeyPart.COLQ, colFilter));
//    return withItersRemote(dis);
//  }

  public TableConfig getTableConfig() {
    return tableConfig;
  }
  public Authorizations getAuthorizations() {
    return authorizations;
  }
  public DynamicIteratorSetting getItersRemote() {
    return DynamicIteratorSetting.fromMap(itersRemote);
  }
  public DynamicIteratorSetting getItersClientSide() {
    return DynamicIteratorSetting.fromMap(itersClientSide);
  }
  public String getRowFilter() {
    return rowFilter;
  }
  public String getColFilter() {
    return colFilter;
  }
  public SortedSet<Range> getRowFilterRanges() {
    return rowFilterRanges;
  }
  public SortedSet<Range> getColFilterRanges() {
    return colFilterRanges;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    InputTableConfig that = (InputTableConfig) o;

    if (!tableConfig.equals(that.tableConfig)) return false;
    if (!authorizations.equals(that.authorizations)) return false;
    if (!itersRemote.equals(that.itersRemote)) return false;
    if (!itersClientSide.equals(that.itersClientSide)) return false;
    if (!rowFilter.equals(that.rowFilter)) return false;
    return colFilter.equals(that.colFilter);

  }

  private transient Integer hashCode = 0; // lazy caching
  @Override
  public int hashCode() {
    if (hashCode != 0) // ignoring super-rare case when the hashCode calculates to 0
      return hashCode;
    int result = tableConfig.hashCode();
    result = 31 * result + authorizations.hashCode();
    result = 31 * result + itersRemote.hashCode();
    result = 31 * result + itersClientSide.hashCode();
    result = 31 * result + rowFilter.hashCode();
    result = 31 * result + colFilter.hashCode();
    return hashCode = result;
  }

  ///////////////////////////////
  public Scanner createScannerWithColFilterAndIters() {
    Connector connector = tableConfig.getConnector();
    Scanner scanner;
    try {
      scanner = connector.createScanner(tableConfig.getTableName(), authorizations);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(tableConfig.getTableName() + " does not exist in instance " + tableConfig.getInstanceName(), e);
    }

    DynamicIteratorSetting disRemote = getItersRemote();
    GraphuloUtil.applyGeneralColumnFilter(colFilter, scanner, disRemote, false); // prepend
    if (!disRemote.isEmpty()) {
      scanner.addScanIterator(disRemote.toIteratorSetting());
    }

    return scanner;
    // todo play with how this is used in RSI and OneTable and TwoTable etc.
  }


}
