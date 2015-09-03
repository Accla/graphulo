package edu.mit.ll.graphulo;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.security.Authorizations;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Map;

import static org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;

/**
 * Immutable class representing a table and options passed around when referring to it.
 * Used at the tablet server by RemoteSourceIterator and RemoteWriteIterator.
 *
 */
public final class TableConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String zookeeperHost;
  private final long timeout;
  private final String instanceName;
  private final String tableName;
  private final String username;
  private final transient AuthenticationToken authenticationToken; // clone this
  private final Authorizations authorizations; // immutable and Serializable
  private final String rowRanges; // allow null
  private final String colFilter; // allow null
  private final Map<String,String> itersBefore; // allow null; copy on read, return Collections.unmodifiableMap()
  private final Map<String,String> itersAfter;  // no need for special measues because DIS.buildSettingMap() and .fromMap() make new objects


  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    out.writeObject(authenticationToken.getClass()); // store class information so we can recover the class name on read
    authenticationToken.write(out);
  }

  @SuppressWarnings("unchecked")
  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    Class<? extends AuthenticationToken> authenticationTokenClass = (Class<? extends AuthenticationToken>)in.readObject();
    // small hack that enables setting a final variable
    try {
      Field authField = TableConfig.class.getDeclaredField("authenticationToken");
      authField.setAccessible(true);
      authField.set(this, authenticationTokenClass.newInstance()); // set to specific instance saved in class
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("impossible: field is named authenticationToken", e);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("trouble creating new authenticationToken of class "+authenticationTokenClass, e);
    }
    authenticationToken.readFields(in);
  }


  /**
   *
   * @param cc Use {@link ClientConfiguration#loadDefault()} to read standard client.conf and related files.
   */
  public TableConfig(ClientConfiguration cc, String tableName, String username,
                     AuthenticationToken authenticationToken) {
    this(cc.get(ClientProperty.INSTANCE_ZK_HOST), cc.get(ClientProperty.INSTANCE_NAME),
        AccumuloConfiguration.getTimeInMillis(cc.get(ClientProperty.INSTANCE_ZK_TIMEOUT)),
        tableName, username, authenticationToken);
  }

  public TableConfig(String zookeeperHost, String instanceName, String tableName, String username,
                     AuthenticationToken authenticationToken) {
    this(zookeeperHost, instanceName,
        AccumuloConfiguration.getTimeInMillis(ClientProperty.INSTANCE_ZK_HOST.getDefaultValue()),
        tableName, username, authenticationToken);
  }

  public TableConfig(String zookeeperHost, String instanceName, long timeout, String tableName, String username,
                     AuthenticationToken authenticationToken) {
    Preconditions.checkNotNull(instanceName,
        "No instance name provided and none found in the default ClientConfiguration (usually loaded from a client.conf file)");
    this.instanceName = instanceName; // default: null
    Preconditions.checkNotNull(zookeeperHost);
    this.zookeeperHost = zookeeperHost; // default: "localhost:2181"
    this.timeout = timeout; // default: "30s"
    Preconditions.checkNotNull(tableName);
    this.tableName = tableName;
    Preconditions.checkNotNull(username);
    this.username = username;
    Preconditions.checkNotNull(authenticationToken, "authenticationToken must be specified not null for user %s", username);
    this.authenticationToken = authenticationToken.clone();
    authorizations = Authorizations.EMPTY;
    rowRanges = null;
    colFilter = null;
    itersAfter = itersBefore = null;
  }

  private TableConfig(String zookeeperHost, long timeout, String instanceName,
                     String tableName, String username, AuthenticationToken authenticationToken,
                      Authorizations authorizations, String rowRanges, String colFilter,
                      Map<String, String> itersBefore, Map<String, String> itersAfter) {
    this.zookeeperHost = zookeeperHost;
    this.timeout = timeout;
    this.instanceName = instanceName;
    this.tableName = tableName;
    this.username = username;
    this.authenticationToken = authenticationToken.clone();
    this.authorizations = authorizations;
    this.rowRanges = rowRanges;
    this.colFilter = colFilter;
    this.itersBefore = itersBefore;
    this.itersAfter = itersAfter;
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    authenticationToken.destroy(); // destroy when this class is about to be garbage collected // PERF
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TableConfig that = (TableConfig) o;

    if (timeout != that.timeout) return false;
    if (!zookeeperHost.equals(that.zookeeperHost)) return false;
    if (!instanceName.equals(that.instanceName)) return false;
    if (!tableName.equals(that.tableName)) return false;
    if (!username.equals(that.username)) return false;
    if (!authenticationToken.equals(that.authenticationToken)) return false;
    if (!authorizations.equals(that.authorizations)) return false;
    if (rowRanges != null ? !rowRanges.equals(that.rowRanges) : that.rowRanges != null) return false;
    if (colFilter != null ? !colFilter.equals(that.colFilter) : that.colFilter != null) return false;
    if (itersBefore != null ? !itersBefore.equals(that.itersBefore) : that.itersBefore != null) return false;
    return !(itersAfter != null ? !itersAfter.equals(that.itersAfter) : that.itersAfter != null);

  }

  @Override
  public int hashCode() {
    int result = zookeeperHost.hashCode();
    result = 31 * result + (int) (timeout ^ (timeout >>> 32));
    result = 31 * result + instanceName.hashCode();
    result = 31 * result + tableName.hashCode();
    result = 31 * result + username.hashCode();
    result = 31 * result + authenticationToken.hashCode();
    result = 31 * result + authorizations.hashCode();
    result = 31 * result + (rowRanges != null ? rowRanges.hashCode() : 0);
    result = 31 * result + (colFilter != null ? colFilter.hashCode() : 0);
    result = 31 * result + (itersBefore != null ? itersBefore.hashCode() : 0);
    result = 31 * result + (itersAfter != null ? itersAfter.hashCode() : 0);
    return result;
  }

  public TableConfig withInstanceName(String instanceName) {
    Preconditions.checkNotNull(instanceName);
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken, authorizations, rowRanges, colFilter,
        itersBefore, itersAfter);
  }

  public TableConfig withZookeeperTimeout(long timeout) {
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken, authorizations, rowRanges, colFilter,
        itersBefore, itersAfter);
  }

  public TableConfig withZookeeperHost(String zookeeperHost) {
    Preconditions.checkNotNull(zookeeperHost);
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken, authorizations, rowRanges, colFilter,
        itersBefore, itersAfter);
  }

  public TableConfig withTableName(String tableName) {
    Preconditions.checkNotNull(tableName);
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken, authorizations, rowRanges, colFilter,
        itersBefore, itersAfter);
  }
  public TableConfig withUsername(String username) {
    Preconditions.checkNotNull(username);
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken, authorizations, rowRanges, colFilter,
        itersBefore, itersAfter);
  }
  public TableConfig withAuthenticationToken(AuthenticationToken authenticationToken) {
    Preconditions.checkNotNull(authenticationToken);
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken, authorizations, rowRanges, colFilter,
        itersBefore, itersAfter);
  }
  public TableConfig withAuthorizations(Authorizations authorizations) {
    if (authorizations == null) authorizations = Authorizations.EMPTY;
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken, authorizations, rowRanges, colFilter,
        itersBefore, itersAfter);
  }
  public TableConfig withRowRanges(String rowRanges) {
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken, authorizations, rowRanges, colFilter,
        itersBefore, itersAfter);
  }
  public TableConfig withColFilter(String colFilter) {
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken, authorizations, rowRanges, colFilter,
        itersBefore, itersAfter);
  }
  public TableConfig withItersBefore(DynamicIteratorSetting itersBefore) {
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken, authorizations, rowRanges, colFilter,
        itersBefore == null ? null : itersBefore.buildSettingMap(), itersAfter);
  }
  public TableConfig withItersAfter(DynamicIteratorSetting itersAfter) {
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken, authorizations, rowRanges, colFilter,
        itersBefore, itersAfter == null ? null : itersAfter.buildSettingMap());
  }

  public String getZookeeperHost() {
    return zookeeperHost;
  }

  public long getTimeout() {
    return timeout;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getUsername() {
    return username;
  }

  public AuthenticationToken getAuthenticationToken() {
    return authenticationToken.clone(); // don't leak the token
  }

  public Authorizations getAuthorizations() {
    return authorizations;
  }

  public String getRowRanges() {
    return rowRanges;
  }

  public String getColFilter() {
    return colFilter;
  }

  public DynamicIteratorSetting getItersBefore() {
    return DynamicIteratorSetting.fromMap(itersBefore);
  }

  public DynamicIteratorSetting getItersAfter() {
    return DynamicIteratorSetting.fromMap(itersAfter);
  }
}
