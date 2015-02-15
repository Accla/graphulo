package testing;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;

/**
 * Provides a MiniAccumuloCluster for use in testing.
 */
public class MiniAccumuloTester extends ExternalResource implements IAccumuloTester
{
    private static final Logger log = LogManager.getLogger(MiniAccumuloTester.class);
    /* Fixture State */
    private File tempDir;
    private MiniAccumuloCluster miniaccumulo;
    private Instance instance;
    private static final String USER = "root";
    private static final String PASSWORD = "password";

    public Connector getConnector() {
        Connector c = null;
        try {
            c = instance.getConnector(USER, new PasswordToken(PASSWORD));
        } catch (AccumuloException | AccumuloSecurityException e) {
            log.error("failed to connect to MiniAccumulo instance",e);
            throw new RuntimeException(e);
        }
        return c;
    }

    @Override
    protected void before() throws Throwable {
        tempDir = Files.createTempDirectory("tempMini",new FileAttribute<?>[] {}).toFile();
        miniaccumulo = new MiniAccumuloCluster(tempDir, PASSWORD);
        miniaccumulo.start();
        instance = new ZooKeeperInstance(miniaccumulo.getInstanceName(), miniaccumulo.getZooKeepers());
        log.debug("setUp ok - instance: " + instance.getInstanceName());
    }

    @Override
    protected void after() {
        instance = null;
        try {
            miniaccumulo.stop();
        } catch (IOException | InterruptedException e) {
            System.err.print("Error stopping MiniAccumuloCluster: ");
            e.printStackTrace();
        }
        tempDir.delete();
        log.debug("tearDown ok - instance destroyed");
    }
}
