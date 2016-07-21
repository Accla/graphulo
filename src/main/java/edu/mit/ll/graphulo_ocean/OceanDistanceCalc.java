package edu.mit.ll.graphulo_ocean;

import com.beust.jcommander.Parameter;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.skvi.TwoTableIterator;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Executable.
 * Ex: java -cp "/home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar" edu.mit.ll.graphulo_ocean.OceanDistanceCalc -listOfSamplesFile "/home/gridsan/dhutchison/gits/istc_oceanography/metadata/valid_samples_GA02_filenames_perm.csv" -everyXLines 2 -startOffset 0 -K 11 -oTsampleDegree oTsampleDegree | tee "$HOME/node-043-ingest.log"
 * createtable oTsampleDist
 */
public class OceanDistanceCalc {
  private static final Logger log = LogManager.getLogger(OceanDistanceCalc.class);

  public static void main(String[] args) {
    new OceanDistanceCalc().execute(args);
  }

  static class Opts extends Help {
    @Parameter(names = {"-oTsampleSeqRaw"}, required = true)
    public String oTsampleSeqRaw = "oTsampleSeqRaw";

    @Parameter(names = {"-txe1"})
    public String txe1 = "classdb54";

    @Parameter(names = {"-oTsampleDegree"}, required = true)
    public String oTsampleDegree;

    @Parameter(names = {"-oTsampleDist"}, required = true)
    public String oTsampleDist;

    @Override
    public String toString() {
      return "Opts{" +
          "oTsampleSeqRaw='" + oTsampleSeqRaw + '\'' +
          ", txe1='" + txe1 + '\'' +
          ", oTsampleDegree='" + oTsampleDegree + '\'' +
          ", oTsampleDist='" + oTsampleDist + '\'' +
          '}';
    }
  }

  public void execute(final String[] args) {
    Opts opts = new Opts();
    opts.parseArgs(OceanDistanceCalc.class.getName(), args);
    log.info(OceanDistanceCalc.class.getName() + " " + opts);

    AuthenticationToken auth = getTXE1Authentication(opts.txe1);
    Connector conn = setupConnector(opts.txe1);
    Graphulo graphulo = new Graphulo(conn, auth);
    executeGraphulo(graphulo, opts);
  }

  void executeGraphulo(Graphulo graphulo, Opts opts) {
    Map<String,String> opt = new HashMap<>();
    opt.put("rowMultiplyOp", DistanceRowMult.class.getName());
//    opt.put("rowMultiplyOp.opt.multiplyOp", multOp.getName()); // treated same as multiplyOp
//    if (multOpOptions != null)
//      for (Map.Entry<String, String> entry : multOpOptions.entrySet()) {
//        opt.put("rowMultiplyOp.opt.multiplyOp.opt."+entry.getKey(), entry.getValue()); // treated same as multiplyOp
//      }
//    opt.put("rowMultiplyOp.opt.rowmode", rowmode.name());

    long l = graphulo.TwoTable(TwoTableIterator.CLONESOURCE_TABLENAME, opts.oTsampleSeqRaw, opts.oTsampleDist, null,
        -1, TwoTableIterator.DOTMODE.ROW, opt, Graphulo.PLUS_ITERATOR_LONG,
        null, null, null,
        false, false, null, null, null,
        null, null,
        -1, null, null);
    log.info("Number of entries written to distance table "+opts.oTsampleDist+": "+l);

    IteratorSetting isDistFinish = DistanceApply.iteratorSetting(Graphulo.DEFAULT_COMBINER_PRIORITY+1,
        graphulo.basicRemoteOpts(ApplyIterator.APPLYOP + GraphuloUtil.OPT_SUFFIX, opts.oTsampleDegree, null, null));
    GraphuloUtil.addOnScopeOption(isDistFinish, EnumSet.of(IteratorUtil.IteratorScope.scan));
    GraphuloUtil.applyIteratorSoft(isDistFinish, graphulo.getConnector().tableOperations(), opts.oTsampleDist);
  }

  private Connector setupConnector(String txe1) {
    String instanceName = txe1;
    String zookeeperHost = txe1+".cloud.llgrid.txe1.mit.edu:2181";
    ClientConfiguration cc = new ClientConfiguration().withInstance(instanceName).withZkHosts(zookeeperHost);// .withZkTimeout(timeout)
    Instance instance = new ZooKeeperInstance(cc);
    AuthenticationToken auth = getTXE1Authentication(txe1);
    try {
      return instance.getConnector("AccumuloUser", auth);
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new RuntimeException("Trouble authenticating to database "+txe1,e);
    }
  }

  private AuthenticationToken getTXE1Authentication(String txe1) {
    File file = new File("/home/gridsan/groups/databases/"+txe1+"/accumulo_user_password.txt");
    PasswordToken token;
    try (BufferedReader is = new BufferedReader(new FileReader(file))) {
      token = new PasswordToken(is.readLine());
    } catch (FileNotFoundException e) {
      log.error("Cannot find accumulo_user_password.txt for instance "+txe1, e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      log.error("Problem reading accumulo_user_password.txt for instance " + txe1, e);
      throw new RuntimeException(e);
    }
    return token;
  }


}
