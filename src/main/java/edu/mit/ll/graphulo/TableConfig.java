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
public final class TableConfig implements Serializable, Cloneable {

  private static final long serialVersionUID = 1L;

  private final String zookeeperHost;
  private final int timeout;
  private final String instanceName;
  private final String tableName;
  private final String username;
  private final transient AuthenticationToken authenticationToken; // clone on creation, clone on get. No need to clone in the middle

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
      set("authenticationToken", authenticationTokenClass.newInstance()); // set to specific instance saved in class
    } catch (InstantiationException e) {
      throw new RuntimeException("trouble creating new authenticationToken of class "+authenticationTokenClass, e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("trouble accessing authenticationToken field for TableConfig "+this, e);
    }
    authenticationToken.readFields(in);
  }

  /**
   * Used to set final fields. Only used immediately after object creation,
   * while only one thread can access the new object.
   */
  private TableConfig set(String field, Object val) {
    try {
      Field f = TableConfig.class.getDeclaredField(field);
      f.setAccessible(true);
      f.set(this, val); // set to specific instance saved in class
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("no TableConfig field named "+field, e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("trouble accessing field "+field+" for TableConfig "+this+" and setting to "+val, e);
    }
    return this;
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

  @Override
  protected TableConfig clone() {
    try {
      return (TableConfig)super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("somehow cannot clone TableConfig "+this, e);
    }
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
    return clone().set("instanceName", Preconditions.checkNotNull(instanceName));
  }
  public TableConfig withZookeeperTimeout(int timeout) {
    return clone().set("timeout", timeout);
  }
  public TableConfig withZookeeperHost(String zookeeperHost) {
    return clone().set("zookeeperHost", Preconditions.checkNotNull(zookeeperHost));
  }
  public TableConfig withTableName(String tableName) {
    return clone().set("tableName", Preconditions.checkNotNull(tableName));
  }
  public TableConfig withUsername(String username) {
    return clone().set("username", Preconditions.checkNotNull(username));
  }
  public TableConfig withAuthenticationToken(AuthenticationToken authenticationToken) {
    return clone().set("authenticationToken", Preconditions.checkNotNull(authenticationToken).clone());
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

  // Calculated lazily, not serialized, derived from other properties.
  // Cloned versions only keep reference after this is set.
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
