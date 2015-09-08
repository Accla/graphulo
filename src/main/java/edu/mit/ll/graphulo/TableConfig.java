package edu.mit.ll.graphulo;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;

import static org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;

/**
 * Immutable class representing a table and options passed around when referring to it.
 * Used at the tablet server by RemoteSourceIterator and RemoteWriteIterator.
 * Convert to an {@link InputTableConfig} or {@link OutputTableConfig}.
 */
public final class TableConfig implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String zookeeperHost;
  private final int timeout;
  private final String instanceName;
  private final String tableName;
  private final String username;
  private final transient AuthenticationToken authenticationToken; // clone this

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
        (int)AccumuloConfiguration.getTimeInMillis(cc.get(ClientProperty.INSTANCE_ZK_TIMEOUT)),
        tableName, username, authenticationToken);
  }

  public TableConfig(String zookeeperHost, String instanceName, String tableName, String username,
                     AuthenticationToken authenticationToken) {
    this(zookeeperHost, instanceName,
        (int)AccumuloConfiguration.getTimeInMillis(ClientProperty.INSTANCE_ZK_TIMEOUT.getDefaultValue()),
        tableName, username, authenticationToken);
  }

  public TableConfig(String zookeeperHost, String instanceName, int timeout, String tableName, String username,
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
  }

  private TableConfig(String zookeeperHost, int timeout, String instanceName,
                      String tableName, String username, AuthenticationToken authenticationToken) {
    this.zookeeperHost = zookeeperHost;
    this.timeout = timeout;
    this.instanceName = instanceName;
    this.tableName = tableName;
    this.username = username;
    this.authenticationToken = authenticationToken.clone();
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
    return authenticationToken.equals(that.authenticationToken);

  }

  @Override
  public int hashCode() {
    int result = zookeeperHost.hashCode();
    result = 31 * result + timeout;
    result = 31 * result + instanceName.hashCode();
    result = 31 * result + tableName.hashCode();
    result = 31 * result + username.hashCode();
    result = 31 * result + authenticationToken.hashCode();
    return result;
  }

  public TableConfig withInstanceName(String instanceName) {
    Preconditions.checkNotNull(instanceName);
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken
    );
  }

  public TableConfig withZookeeperTimeout(int timeout) {
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken
    );
  }

  public TableConfig withZookeeperHost(String zookeeperHost) {
    Preconditions.checkNotNull(zookeeperHost);
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken
    );
  }

  public TableConfig withTableName(String tableName) {
    Preconditions.checkNotNull(tableName);
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken
    );
  }
  public TableConfig withUsername(String username) {
    Preconditions.checkNotNull(username);
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken
    );
  }
  public TableConfig withAuthenticationToken(AuthenticationToken authenticationToken) {
    Preconditions.checkNotNull(authenticationToken);
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken
    );
  }
  public TableConfig withRowRanges(String rowRanges) {
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken
    );
  }
  public TableConfig withColFilter(String colFilter) {
    return new TableConfig(zookeeperHost, timeout, instanceName,
        tableName, username, authenticationToken
    );
  }

  public InputTableConfig asInput() {
    return new InputTableConfig(this);
  }
  public OutputTableConfig asOutput() {
    return new OutputTableConfig(this);
  }

  public String getZookeeperHost() {
    return zookeeperHost;
  }
  public int getTimeout() {
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

  //////////// Calculated lazily, not serialized, derived from other properties
  private transient Connector connector;

  public Connector getConnector() {
    if (connector == null)
      try {
        connector = new ZooKeeperInstance(ClientConfiguration.loadDefault()
            .withInstance(instanceName)
            .withZkTimeout(timeout)
            .withZkHosts(zookeeperHost))
            .getConnector(username, authenticationToken);
      } catch (AccumuloSecurityException | AccumuloException e) {
        throw new RuntimeException("failed to connect to Accumulo instance " + instanceName +" with user "+username, e);
      }
    return connector; // please don't use the deprecated set methods on the Instance from connector.getInstance();
  }

  public boolean exists() {
    return getConnector().tableOperations().exists(tableName);
  }


}
