package edu.mit.ll.graphulo;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import java.io.Serializable;
import java.util.Map;

/**
 * Immutable class representing a table used for input to an iterator stack via a local iterator or a RemoteSourceIterator.
 */
public final class InputTableConfig implements Serializable {

  public static final int DEFAULT_ITERS_REMOTE_PRIORITY = 50;
  static final Map<String,String> DEFAULT_ITERS_MAP =
      new DynamicIteratorSetting(DEFAULT_ITERS_REMOTE_PRIORITY, null).buildSettingMap();

  private static final long serialVersionUID = 1L;

  private final TableConfig tableConfig;
  private final Authorizations authorizations; // immutable and Serializable
  private final Map<String,String> itersRemote; // no null; copy on read, return Collections.unmodifiableMap()
  private final Map<String,String> itersLocal;  // no need for special measures because DIS.buildSettingMap() and .fromMap() make new objects
                                                // combine the two when used as a local iterator as opposed to a RemoteSourceIterator
  private final String rowRanges;    // no null, use equivalence of Ranges instead
  private final String colFilter;    // no null, use equivalence of Ranges instead


  public InputTableConfig(TableConfig tableConfig) {
    Preconditions.checkNotNull(tableConfig);
    this.tableConfig = tableConfig;
    authorizations = Authorizations.EMPTY;
    itersLocal = itersRemote = DEFAULT_ITERS_MAP;
    rowRanges = ":,";
    colFilter = ":,";
  }

  private InputTableConfig(TableConfig tableConfig, Authorizations authorizations,
                           Map<String, String> itersRemote, Map<String, String> itersLocal,
                           String rowRanges, String colFilter) {
    this.tableConfig = tableConfig;
    this.authorizations = authorizations;
    this.itersLocal = itersLocal;
    this.itersRemote = itersRemote;
    this.rowRanges = rowRanges;
    this.colFilter = colFilter;
  }

  public InputTableConfig withTableConfig(TableConfig tableConfig) {
    Preconditions.checkNotNull(tableConfig);
    return new InputTableConfig(tableConfig, authorizations, itersRemote, itersLocal, rowRanges, colFilter);
  }
  public InputTableConfig withAuthorizations(Authorizations authorizations) {
    if (authorizations == null) authorizations = Authorizations.EMPTY;
    return new InputTableConfig(tableConfig, authorizations, itersRemote, itersLocal, rowRanges, colFilter);
  }
  public InputTableConfig withItersRemote(DynamicIteratorSetting itersRemote) {
    return new InputTableConfig(tableConfig, authorizations, itersRemote == null ? DEFAULT_ITERS_MAP : itersRemote.buildSettingMap(), itersLocal, rowRanges, colFilter);
  }
  public InputTableConfig withItersLocal(DynamicIteratorSetting itersLocal) {
    return new InputTableConfig(tableConfig, authorizations, itersRemote, itersLocal == null ? DEFAULT_ITERS_MAP : itersLocal.buildSettingMap(), rowRanges, colFilter);
  }
  public InputTableConfig withRowRanges(String rowRanges) {
    return new InputTableConfig(tableConfig, authorizations, itersRemote, itersLocal, rowRanges == null ? ":," : rowRanges, colFilter);
  }
  public InputTableConfig withColFilter(String colFilter) {
    return new InputTableConfig(tableConfig, authorizations, itersRemote, itersLocal, rowRanges, colFilter == null ? ":," : colFilter);
  }

  // less efficient way to do rowRanges and colFilter
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
  public DynamicIteratorSetting getItersLocal() {
    return DynamicIteratorSetting.fromMap(itersLocal);
  }
  public String getRowRanges() {
    return rowRanges;
  }
  public String getColFilter() {
    return colFilter;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    InputTableConfig that = (InputTableConfig) o;

    if (!tableConfig.equals(that.tableConfig)) return false;
    if (!authorizations.equals(that.authorizations)) return false;
    if (!itersRemote.equals(that.itersRemote)) return false;
    if (!itersLocal.equals(that.itersLocal)) return false;
    if (!Range.mergeOverlapping(GraphuloUtil.d4mRowToRanges(rowRanges)).equals(
        Range.mergeOverlapping(GraphuloUtil.d4mRowToRanges(that.rowRanges)))) return false;
    return Range.mergeOverlapping(GraphuloUtil.d4mRowToRanges(colFilter)).equals(
        Range.mergeOverlapping(GraphuloUtil.d4mRowToRanges(that.colFilter)));

  }

  @Override
  public int hashCode() {
    int result = tableConfig.hashCode();
    result = 31 * result + authorizations.hashCode();
    result = 31 * result + itersRemote.hashCode();
    result = 31 * result + itersLocal.hashCode();
    result = 31 * result + Range.mergeOverlapping(GraphuloUtil.d4mRowToRanges(rowRanges)).hashCode();
    result = 31 * result + Range.mergeOverlapping(GraphuloUtil.d4mRowToRanges(colFilter)).hashCode();
    return result;
  }
}
