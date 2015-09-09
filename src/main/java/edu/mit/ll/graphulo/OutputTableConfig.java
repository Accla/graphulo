package edu.mit.ll.graphulo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import edu.mit.ll.graphulo.apply.ApplyOp;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

/**
 * Immutable class representing a table used as output from an iterator stack via RemoteWriteIterator.
 */
public final class OutputTableConfig implements Serializable, Cloneable {
  private static final long serialVersionUID = 1L;

  public static final int DEFAULT_COMBINER_PRIORITY = 6;
  private static final Map<String,String> DEFAULT_ITERS_MAP =
      ImmutableMap.copyOf(new DynamicIteratorSetting(DEFAULT_COMBINER_PRIORITY, null).buildSettingMap());

  private final TableConfig tableConfig;
  private final Class<? extends ApplyOp> applyLocal;  // allow null
  private final Map<String,String> applyLocalOptions;
  private final Map<String,String> tableItersRemote;

  public OutputTableConfig(TableConfig tableConfig) {
    Preconditions.checkNotNull(tableConfig);
    this.tableConfig = tableConfig;
    applyLocal = null;
    applyLocalOptions = Collections.emptyMap();
    tableItersRemote = DEFAULT_ITERS_MAP;
  }

  /**
   * Used to set final fields. Only used immediately after object creation,
   * while only one thread can access the new object.
   */
  private OutputTableConfig set(String field, Object val) {
    try {
      Field f = OutputTableConfig.class.getDeclaredField(field);
      f.setAccessible(true);
      f.set(this, val); // set to specific instance saved in class
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("no OutputTableConfig field named "+field, e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("trouble accessing field "+field+" for TableConfig "+this+" and setting to "+val, e);
    }
    return this;
  }

  @Override
  protected OutputTableConfig clone() {
    try {
      return (OutputTableConfig)super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("somehow cannot clone OutputTableConfig "+this, e);
    }
  }

  public OutputTableConfig withTableConfig(TableConfig tableConfig) {
    return clone().set("tableConfig", Preconditions.checkNotNull(tableConfig));
  }
  public OutputTableConfig withApplyLocal(Class<? extends ApplyOp> applyLocal, Map<String,String> applyLocalOptions) {
    return clone().set("applyLocal", applyLocal)
        .set("applyLocalOptions", applyLocal == null || applyLocalOptions == null ? Collections.<String,String>emptyMap() : ImmutableMap.copyOf(applyLocalOptions));
  }
  public OutputTableConfig withTableItersRemote(DynamicIteratorSetting tableItersRemote) {
    return clone().set("tableItersRemote", tableItersRemote == null ? DEFAULT_ITERS_MAP : tableItersRemote.buildSettingMap());
  }

  // will enable these shortcut methods if determined to be a safe, common use case
//  public InputTableConfig withRowFilter(String rowFilter) {
//    DynamicIteratorSetting dis = DynamicIteratorSetting.fromMap(tableItersRemote);
//    dis.prepend(D4mRangeFilter.iteratorSetting(1, D4mRangeFilter.KeyPart.ROW, rowFilter));
//    return withItersRemote(dis);
//  }
//  public InputTableConfig withColFilter(String colFilter) {
//    DynamicIteratorSetting dis = DynamicIteratorSetting.fromMap(tableItersRemote);
//    dis.prepend(D4mRangeFilter.iteratorSetting(1, D4mRangeFilter.KeyPart.COLQ, colFilter));
//    return withItersRemote(dis);
//  }

  public TableConfig getTableConfig() {
    return tableConfig;
  }
  public DynamicIteratorSetting getTableItersRemote() {
    return DynamicIteratorSetting.fromMap(tableItersRemote);
  }
  public Class<? extends ApplyOp> getApplyLocal() {
    return applyLocal;
  }
  public Map<String, String> getApplyLocalOptions() {
    return applyLocalOptions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OutputTableConfig that = (OutputTableConfig) o;

    if (!tableConfig.equals(that.tableConfig)) return false;
    if (applyLocal != null ? !applyLocal.equals(that.applyLocal) : that.applyLocal != null) return false;
    if (!applyLocalOptions.equals(that.applyLocalOptions)) return false;
    return tableItersRemote.equals(that.tableItersRemote);

  }

  @Override
  public int hashCode() {
    int result = tableConfig.hashCode();
    result = 31 * result + (applyLocal != null ? applyLocal.hashCode() : 0);
    result = 31 * result + applyLocalOptions.hashCode();
    result = 31 * result + tableItersRemote.hashCode();
    return result;
  }

  /**
   * Applies the DynamicIterator stored as tableItersRemote to the remote table on all scopes (scan, minc, majc).
   * No effect for a scope if an iterator already exists at the same priority and name.
   * @throws RuntimeException if another iterator already exists at the same priority or the same name
   */
  public void applyRemoteIterators() {
    Connector connector = tableConfig.getConnector();
    DynamicIteratorSetting dis = getTableItersRemote();
    if (!dis.isEmpty()) {
      IteratorSetting itset = dis.toIteratorSetting();
      GraphuloUtil.applyIteratorSoft(itset, connector.tableOperations(), tableConfig.getTableName());
    }
  }

}
