package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.KnownBugSuite;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

@RunWith(KnownBugSuite.class)
@Suite.SuiteClasses({
    UtilTest.class,
    InjectTest.class,
    RemoteIteratorTest.class,
    TableMultIteratorTest.class,
    TableMultTest.class,
    TableFilterTest.class,
    BFSTest.class,
    CountTest.class,
    RowMultiplyTest.class,
    ReducerTest.class,
    EWiseTest.class,
    SCCTest.class,
    LineTest.class,
    AlgorithmTest.class
})

/**
 * Holds test configurations; default test suite for JUnit testing.
 */
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
    if (s.startsWith("txe1-")) {
      String instance  = s.substring(5);
      File file = new File("/home/gridsan/groups/databases/"+instance+"/accumulo_user_password.txt");
      PasswordToken token;
      try (BufferedReader is = new BufferedReader(new FileReader(file))) {
        token = new PasswordToken(is.readLine());
      } catch (FileNotFoundException e) {
        log.error("Cannot find accumulo_user_password.txt for instance "+instance, e);
        throw new RuntimeException(e);
      } catch (IOException e) {
        log.error("Problem reading accumulo_user_password.txt for instance " + instance, e);
        throw new RuntimeException(e);
      }
      AccumuloTester = new RealAccumuloTester(instance, instance+".cloud.llgrid.txe1.mit.edu:2181", 5000, "AccumuloUser", token);

    } else {
      switch (s) {
        case "local":
          AccumuloTester = new RealAccumuloTester("instance", "localhost:2181", 5000, "root", new PasswordToken("secret"));
          break;
        case "local-1.7.0":
          // G = edu.mit.ll.graphulo.MatlabGraphulo('instance-1.7.0','localhost:2181','root','secret')
          // DB = DBserver('localhost:2181','Accumulo','instance-1.7.0','root','secret')
          AccumuloTester = new RealAccumuloTester("instance-1.7.0", "localhost:2181", 5000, "root", new PasswordToken("secret"));
          break;
        case "mini":
          AccumuloTester = new MiniAccumuloTester(1, false, false);
          break;
        case "miniDebug":   // Enables debugging on started MiniAccumulo process.
          AccumuloTester = new MiniAccumuloTester(1, true, false);
          break;
        case "mini2": // 2 tablet server MiniAccumuloCluster
          AccumuloTester = new MiniAccumuloTester(2, false, false);
          break;
        case "miniReuse":
          AccumuloTester = new MiniAccumuloTester(2, false, true);
          break;
        default:
          log.warn("Using \"mini\" due to unrecognized TEST_ACCUMULO option: " + s);
          AccumuloTester = new MiniAccumuloTester(1, false, false);
          break;
      }
    }
  }

  // Alternatives:
//    public static final IAccumuloTester AccumuloTester =
//            new RealAccumuloTester("instance","localhost:2181",5000,"root",new PasswordToken("secret"));

  //"classdb51.cloud.llgrid.txe1.mit.edu:2181"

}
