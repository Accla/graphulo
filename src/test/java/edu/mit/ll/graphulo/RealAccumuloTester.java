package testing;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.rules.ExternalResource;

public class RealAccumuloTester extends ExternalResource implements IAccumuloTester {
    private static final Logger log = LogManager.getLogger(RealAccumuloTester.class);

    private ClientConfiguration cc;
    private String username = "root";
    private AuthenticationToken auth;

    private Instance instance;

    public RealAccumuloTester(String instanceName, String zookeeperHost, int timeout,
                              String username, AuthenticationToken auth) {
        cc = ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeeperHost).withZkTimeout(timeout);
        this.username = username;
        this.auth = auth;
    }

    public Connector getConnector() {
        Connector c = null;

        try {
            c = instance.getConnector(username, auth);
        } catch (AccumuloException | AccumuloSecurityException e) {
            log.error("failed to connect to MiniAccumulo instance",e);
            throw new RuntimeException(e);
        }
        return c;
    }

    @Override
    protected void before() throws Throwable {
        instance = new ZooKeeperInstance(cc.get(ClientConfiguration.ClientProperty.INSTANCE_NAME),
                                    cc.get(ClientConfiguration.ClientProperty.INSTANCE_ZK_HOST));
        log.debug("setUp ok - ClientConfiguration="+cc+" instance=" + instance.getInstanceName());
    }

//    @Override
//    protected void after() {
//        //log.debug("tearDown ok - instance destroyed");
//    }
}
