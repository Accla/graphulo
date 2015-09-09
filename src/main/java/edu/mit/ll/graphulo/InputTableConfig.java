package edu.mit.ll.graphulo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
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
public final class InputTableConfig implements Serializable, Cloneable {
  private static final long serialVersionUID = 1L;

  public static final int DEFAULT_ITERS_REMOTE_PRIORITY = 50;
  private static final Map<String,String> DEFAULT_ITERS_MAP =
      ImmutableMap.copyOf(new DynamicIteratorSetting(DEFAULT_ITERS_REMOTE_PRIORITY, null).buildSettingMap());
  private static final SortedSet<Range> ALL_RANGE = ImmutableSortedSet.of(new Range()); // please do not call the readFields() method of the range inside
  private static final String ALL_RANGE_STR = ":,";

  private final TableConfig tableConfig;
  private final Authorizations authorizations; // immutable and Serializable
  private final Map<String,String> itersRemote;     // no null; copy on read, return ImmutableMap. Controls priority.
  private final Map<String,String> itersClientSide; // no need for special measures because DIS.buildSettingMap() and .fromMap() make new objects
                                                // combine the two when used as a local iterator as opposed to a RemoteSourceIterator
  private final String rowFilter, colFilter;    // no null, always store in sorted merged form, always keep aligned with the SortedSet<Range> versions
  private final transient SortedSet<Range> rowFilterRanges, colFilterRanges; // ^^

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    set("rowFilterRanges", GraphuloUtil.d4mRowToRanges(rowFilter));
    set("colFilterRanges", GraphuloUtil.d4mRowToRanges(colFilter));
  }

  /**
   * Used to set final fields. Only used immediately after object creation,
   * while only one thread can access the new object.
   */
  private InputTableConfig set(String field, Object val) {
    try {
      Field f = InputTableConfig.class.getDeclaredField(field);
      f.setAccessible(true);
      f.set(this, val); // set to specific instance saved in class
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("no InputTableConfig field named "+field, e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("trouble accessing field "+field+" for TableConfig "+this+" and setting to "+val, e);
    }
    return this;
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

  @Override
  protected InputTableConfig clone() {
    try {
      return (InputTableConfig)super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("somehow cannot clone InputTableConfig "+this, e);
    }
  }

  public InputTableConfig withTableConfig(TableConfig tableConfig) {
    return clone().set("tableConfig", Preconditions.checkNotNull(tableConfig));
  }
  public InputTableConfig withAuthorizations(Authorizations authorizations) {
    return clone().set("authorizations", Preconditions.checkNotNull(authorizations));
  }
  public InputTableConfig withItersRemote(DynamicIteratorSetting itersRemote) {
    return clone().set("itersRemote", itersRemote == null ? DEFAULT_ITERS_MAP : itersRemote.buildSettingMap());
  }
  /** Use a single remote iterator. */
  public InputTableConfig withItersRemote(IteratorSetting iterRemote) {
    return withItersRemote(DynamicIteratorSetting.of(iterRemote));
  }
  public InputTableConfig withItersLocal(DynamicIteratorSetting itersLocal) {
    return clone().set("itersLocal", itersLocal == null ? DEFAULT_ITERS_MAP : itersLocal.buildSettingMap());
  }
  /** Use a single local iterator. */
  public InputTableConfig withItersLocal(IteratorSetting iterLocal) {
    return withItersLocal(DynamicIteratorSetting.of(iterLocal));
  }
  public InputTableConfig withRowFilter(String rowFilter) {
    if (rowFilter == null || rowFilter.isEmpty())
      return clone()
          .set("rowFilter", ALL_RANGE_STR)
          .set("rowFilterRanges", ALL_RANGE);
    else {
      SortedSet<Range> set = ImmutableSortedSet.copyOf(Range.mergeOverlapping(GraphuloUtil.d4mRowToRanges(rowFilter)));
      rowFilter = GraphuloUtil.rangesToD4MString(set);
      return clone()
          .set("rowFilter", rowFilter)
          .set("rowFilterRanges", set);
    }
  }
  public InputTableConfig withColFilter(String colFilter) {
    if (colFilter == null || colFilter.isEmpty())
      return clone()
          .set("colFilter", ALL_RANGE_STR)
          .set("colFilterRanges", ALL_RANGE);
    else {
      SortedSet<Range> set = ImmutableSortedSet.copyOf(Range.mergeOverlapping(GraphuloUtil.d4mRowToRanges(colFilter)));
      colFilter = GraphuloUtil.rangesToD4MString(set);
      return clone()
          .set("colFilter", colFilter)
          .set("colFilterRanges", set);
    }
  }
  public InputTableConfig withRowFilter(Collection<Range> rowFilterRanges) {
    Preconditions.checkArgument(rowFilterRanges != null && !rowFilterRanges.isEmpty());
    SortedSet<Range> rset = ImmutableSortedSet.copyOf(Range.mergeOverlapping(rowFilterRanges));
    return clone()
        .set("rowFilter", GraphuloUtil.rangesToD4MString(rset))
        .set("rowFilterRanges", rset);
  }
  public InputTableConfig withColFilter(Collection<Range> colFilterRanges) {
    Preconditions.checkArgument(colFilterRanges != null && !colFilterRanges.isEmpty());
    SortedSet<Range> rset = ImmutableSortedSet.copyOf(Range.mergeOverlapping(colFilterRanges));
    return clone()
        .set("colFilter", GraphuloUtil.rangesToD4MString(rset))
        .set("colFilterRanges", rset);
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
