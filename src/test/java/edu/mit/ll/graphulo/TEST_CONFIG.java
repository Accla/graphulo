package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.KnownBugSuite;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(KnownBugSuite.class)
@Suite.SuiteClasses({
        InjectTest.class,
        RemoteIteratorTest.class,
        TableMultIteratorTest.class,
        TableMultTest.class
})

public class TEST_CONFIG {
    private static final Logger log = LogManager.getLogger(TEST_CONFIG.class);
    /**
     * Set the Accumulo config to use for all test classes here.
     */
    public static IAccumuloTester AccumuloTester;

    static {
        String s = System.getProperty("TEST_CONFIG"); // environment variable
        if (s == null)
            s = "mini";
        switch (s) {
            case "local":
                AccumuloTester = new RealAccumuloTester("instance", "localhost:2181", 5000, "root", new PasswordToken("secret"));
                break;
            case "local-1.7.0":
                AccumuloTester = new RealAccumuloTester("instance-1.7.0", "localhost:2181", 5000, "root", new PasswordToken("secret"));
                break;
            case "txe1":
                AccumuloTester = new RealAccumuloTester("classdb51", "classdb51.cloud.llgrid.txe1.mit.edu:2181", 5000, "root", new PasswordToken("secret"));
                break;
            case "mini":
                AccumuloTester = new MiniAccumuloTester();
                break;
            case "mini2": // 2 tablet server MiniAccumuloCluster
                AccumuloTester = new MiniAccumuloTester(2);
                break;
            default:
                log.warn("Using \"mini\" due to unrecognized TEST_ACCUMULO option: " + s);
                AccumuloTester = new MiniAccumuloTester();
                break;
        }
    }

    // Alternatives:
//    public static final IAccumuloTester AccumuloTester =
//            new RealAccumuloTester("instance","localhost:2181",5000,"root",new PasswordToken("secret"));

    //"classdb51.cloud.llgrid.txe1.mit.edu:2181"

}
