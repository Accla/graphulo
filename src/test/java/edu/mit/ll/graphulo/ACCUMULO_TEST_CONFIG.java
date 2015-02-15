package testing;

import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ACCUMULO_TEST_CONFIG {
    private static final Logger log = LogManager.getLogger(ACCUMULO_TEST_CONFIG.class);
    /**
     * Set the Accumulo config to use for all test classes here.
     */
    public static IAccumuloTester AccumuloTester;

    static {
        String s = System.getProperty("TEST_ACCUMULO");
        if (s == null)
            s = "mini";
        switch(s) {
            case "local":
                AccumuloTester = new RealAccumuloTester("instance","localhost:2181",5000,"root",new PasswordToken("secret"));
                break;
            case "txe1":
                AccumuloTester = new RealAccumuloTester("classdb51","classdb51.cloud.llgrid.txe1.mit.edu:2181",5000,"root",new PasswordToken("secret"));
                break;
            case "mini":
                AccumuloTester = new MiniAccumuloTester();
                break;
            default:
                log.warn("Using \"mini\" due to unrecognized TEST_ACCUMULO option: "+s);
                AccumuloTester = new MiniAccumuloTester();
                break;
        }
    }

    // Alternatives:
//    public static final IAccumuloTester AccumuloTester =
//            new RealAccumuloTester("instance","localhost:2181",5000,"root",new PasswordToken("secret"));

    //"classdb51.cloud.llgrid.txe1.mit.edu:2181"

}
