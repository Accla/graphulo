package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.mult.BigDecimalMultiply;
import edu.mit.ll.graphulo.mult.LongMultiply;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * Matlab interface to Graphulo.
 */
public class MatlabGraphulo extends Graphulo {

  static {
    // load log4j once, when this class is loaded
    DOMConfigurator.configure(MatlabGraphulo.class.getClassLoader().getResource("log4j.xml"));
  }

  public MatlabGraphulo(String instanceName, String zookeepers, String username, String password)
      throws AccumuloSecurityException, AccumuloException {
    super(new ZooKeeperInstance(instanceName, zookeepers).getConnector(username, new PasswordToken(password)), new PasswordToken(password));
  }

  public void TableMultTest(String Ptable,
                            String Atable, String BTtable) {
    TableMultTest(Ptable, Atable, BTtable, null, null, true);
  }

  public void TableMultTest(String Ptable,
                            String Atable, String BTtable,
                            String Ctable, String Rtable, boolean wait) {
    TableMult(Ptable, Atable, BTtable,
        //BigDecimalMultiply.class, BigDecimalCombiner.BigDecimalSummingCombiner.class,
        LongMultiply.class, SummingCombiner.class,
        null, null,
        Ctable, Rtable, wait);
  }
  public void TableMultTest(String Ptable,
                            String Atable, String BTtable,
                            String Ctable, String Rtable, boolean wait, boolean trace) {
    TableMult(Ptable, Atable, BTtable,
        //BigDecimalMultiply.class, BigDecimalCombiner.BigDecimalSummingCombiner.class,
        LongMultiply.class, SummingCombiner.class,
        null, null,
        Ctable, Rtable, wait, trace);
  }



}
