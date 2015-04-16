package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.OfflineScanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;


import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * Graphulo operation implementation.
 */
public class Graphulo implements IGraphulo {
  private static final Logger log = LogManager.getLogger(Graphulo.class);

  private Connector connector;
  private PasswordToken password;

  public Graphulo(Connector connector, PasswordToken password) {
    this.connector = connector;
    this.password = password;
    checkCredentials();
  }

  /** Check password works for this user. */
  private void checkCredentials() {
    try {
      if (!connector.securityOperations().authenticateUser(connector.whoami(), password))
        throw new IllegalArgumentException("instance "+connector.getInstance().getInstanceName()+": bad username "+connector.whoami()+" with password "+new String(password.getPassword()));
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new IllegalArgumentException("instance "+connector.getInstance().getInstanceName()+": error with username "+connector.whoami()+" with password "+new String(password.getPassword()), e);
    }
  }

  @Override
  public void TableMult(String ATtable, String Btable, String Ctable,
                        Class<? extends IMultiplyOp> multOp, Class<? extends Combiner> sumOp,
                        Collection<Range> rowFilter,
                        Collection<IteratorSetting.Column> colFilter) {
    TableMult(ATtable, Btable, Ctable, multOp, sumOp, rowFilter, colFilter, 250000, true);
  }

  public void TableMult(String ATtable, String Btable, String Ctable,
                        Class<? extends IMultiplyOp> multOp, Class<? extends Combiner> sumOp,
                        Collection<Range> rowFilter,
                        Collection<IteratorSetting.Column> colFilter,
                        int numEntriesCheckpoint, boolean trace) {
    if (ATtable == null || ATtable.isEmpty())
      throw new IllegalArgumentException("Please specify table A. Given: "+ATtable);
    if (Btable == null || Btable.isEmpty())
      throw new IllegalArgumentException("Please specify table BT. Given: "+Btable);
    if (ATtable.equals(Ctable))
      log.warn("Untested combination: ATtable=Ctable="+ATtable);
    if (Btable.equals(Ctable))
      log.warn("Untested combination: Btable=Ctable="+Btable);

    if (rowFilter != null && !rowFilter.isEmpty())
      throw new UnsupportedOperationException("rowFilter is not yet implemented; given: "+rowFilter);
    if (colFilter != null && !colFilter.isEmpty())
      throw new UnsupportedOperationException("colFilter is not yet implemented; given: "+colFilter);
//    if (multOp == null || !multOp.equals(BigDecimalMultiply.class))
//      throw new UnsupportedOperationException("only supported multOp is BigDecimalMultiply, but given: "+multOp);
//    if (sumOp == null || !sumOp.equals(BigDecimalCombiner.BigDecimalSummingCombiner.class))
//      throw new UnsupportedOperationException("only supported sumOp is BigDecimalSummingCombiner, but given: "+multOp);

    TableOperations tops = connector.tableOperations();
    if (!tops.exists(ATtable))
      throw new IllegalArgumentException("Table AT does not exist. Given: "+ATtable);
    if (!tops.exists(Btable))
      throw new IllegalArgumentException("Table B does not exist. Given: "+Btable);

    if (Ctable != null && !Ctable.isEmpty() && !tops.exists(Ctable))
      try {
        tops.create(Ctable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("error trying to create C table "+Ctable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("impossible",e);
        throw new RuntimeException(e);
      }

    String instance = connector.getInstance().getInstanceName();
    String zookeepers = connector.getInstance().getZooKeepers();
    String user = connector.whoami();

    Map<String,String> opt = new HashMap<>();
    opt.put("trace",String.valueOf(trace)); // enable distributed tracer

    opt.put("AT.zookeeperHost", zookeepers);
    opt.put("AT.instanceName", instance);
    opt.put("AT.tableName", ATtable);
    opt.put("AT.username", user);
    opt.put("AT.password", new String(password.getPassword()));
//    opt.put("B.zookeeperHost", zookeepers);
//    opt.put("B.instanceName", instance);
//    opt.put("B.tableName", Btable);
//    opt.put("B.username", user);
//    opt.put("B.password", new String(password.getPassword()));

    if (Ctable != null && !Ctable.isEmpty()) {
      opt.put("C.zookeeperHost", zookeepers);
      opt.put("C.instanceName", instance);
      opt.put("C.tableName", Ctable);
      opt.put("C.username", user);
      opt.put("C.password", new String(password.getPassword()));
      opt.put("C.numEntriesCheckpoint", String.valueOf(numEntriesCheckpoint)); // TODO P1: hard-coded numEntriesCheckpoint
    }

    // attach combiner on Ctable
    // TODO P2: Assign priority and name dynamically, checking for conflicts.
    Map<String, String> optSum = new HashMap<>();
    optSum.put("all", "true");
    IteratorSetting iSum = new IteratorSetting(19,BigDecimalCombiner.BigDecimalSummingCombiner.class, optSum);
    try {
      tops.attachIterator(Ctable, iSum);
    } catch (AccumuloSecurityException | AccumuloException e) {
      log.error("error trying to add BigDecimalSummingCombiner to " + Ctable, e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    }

    String ATtableClone = ATtable+"_2";
    String BtableClone = Btable+"_2";
//    try {
//      tops.clone(ATtable, ATtableClone, true, null, null);
//    } catch (AccumuloSecurityException | AccumuloException e) {
//      log.error("error trying to add clone "+ATtable+" to " + ATtableClone, e);
//      throw new RuntimeException(e);
//    } catch (TableNotFoundException e) {
//      log.error("impossible", e);
//      throw new RuntimeException(e);
//    } catch (TableExistsException e) {
//      log.error("table " + ATtableClone+" already exists", e);
//      throw new RuntimeException(e);
//    }
    try {
      tops.clone(Btable, BtableClone, true, null, null);
    } catch (AccumuloSecurityException | AccumuloException e) {
      log.error("error trying to add clone "+Btable+" to " + BtableClone, e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    } catch (TableExistsException e) {
      log.error("table " + BtableClone+" already exists", e);
      throw new RuntimeException(e);
    }
    try {
      long st = System.currentTimeMillis();
      tops.offline(BtableClone, true);
      long dur = System.currentTimeMillis() - st;
      System.out.println("offline "+BtableClone+" time: "+dur);
    } catch (AccumuloSecurityException | AccumuloException e) {
      log.error("error trying to offline " + BtableClone, e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    }
//    Configuration hadoopConf = new Configuration(true);
//    hadoopConf.set("fs.hdfs.impl",
//        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
//    );
    try {
      this.getClass().getClassLoader().loadClass("org.apache.hadoop.hdfs.DistributedFileSystem");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    Configuration.addDefaultResource("file:///home/dhutchis/opt/stack/hadoop-2.6.0/etc/hadoop/core-site.xml");

//    hadoopConfig.set("fs.hdfs.impl",
//        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
//    );
    OfflineScanner bs = new OfflineScanner(connector.getInstance(), new Credentials(connector.whoami(), password),
        connector.tableOperations().tableIdMap().get(BtableClone), Authorizations.EMPTY);

    // scan B with TableMultIteratorQuery
//    BatchScanner bs;
//    try {
//      bs = connector.createBatchScanner(Btable, Authorizations.EMPTY, 2); // TODO P2: set number of batch scan threads
//    } catch (TableNotFoundException e) {
//      log.error("impossible", e);
//      throw new RuntimeException(e);
//    }
    // TODO P2: Assign priority and name dynamically, checking for conflicts.
    IteratorSetting itset = new IteratorSetting(2, TableMultIteratorQuery.class, opt);
    bs.addScanIterator(itset);
//    bs.setRanges(Collections.singleton(new Range()));

    for (Map.Entry<Key, Value> entry : bs) {
      if (Ctable != null && !Ctable.isEmpty()) {
        Value v = entry.getValue();
        ByteBuffer bb = ByteBuffer.wrap(v.get());
        int numEntries = bb.getInt();
        char c = bb.getChar();
        String str = new Value(bb).toString();
        System.out.println(entry.getKey().toString() + " -> " + numEntries + c + str);
      }
      else {
        System.out.println(entry.getKey().toString() + " -> " + entry.getValue());
      }
      // TODO P1: change to method Matlab grabs data from a table
    }
  }

  public void CancelCompact(String table) {
    try {
      connector.tableOperations().cancelCompaction(table);
    } catch (AccumuloException | AccumuloSecurityException e) {
      log.error("error trying to cancel compaction for " + table, e);
    } catch (TableNotFoundException e) {
      log.error("", e);
    }
  }

  public void testReadWriteA(String Ptable, String Atable) {
    TableOperations tops = connector.tableOperations();
    if (!tops.exists(Ptable))
      try {
        tops.create(Ptable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("error trying to create P table "+Ptable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("impossible",e);
        throw new RuntimeException(e);
      }

    String instance = connector.getInstance().getInstanceName();
    String zookeepers = connector.getInstance().getZooKeepers();
    String user = connector.whoami();

    Map<String,String> opt = new HashMap<>();
    opt.put("zookeeperHost", zookeepers);
    opt.put("instanceName", instance);
    opt.put("tableName", Atable);
    opt.put("username", user);
    opt.put("password", new String(password.getPassword()));
    opt.put("doWholeRow", "true");
    IteratorSetting itset = new IteratorSetting(2, RemoteSourceIterator.class, opt);
    try {
      //tops.attachIterator(Ptable, itset);
      long t1 = System.currentTimeMillis();
      // flush, block
      tops.compact(Ptable, null, null, Collections.singletonList(itset), true, true);
      long t2 = System.currentTimeMillis();
      log.info("Time for blocking compact() call to return: " + (t2 - t1) / 1000.0);
    } catch (AccumuloException e) {
      log.error("error trying to compact "+Ptable+" with TableMultIterator; is the iterator installed on the Accumulo server?", e);
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      log.error("error trying to compact "+Ptable+" with TableMultIterator", e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    }
  }

  public void testReadOnlyA(String Ptable, String Atable) {
    TableOperations tops = connector.tableOperations();
    if (!tops.exists(Ptable))
      try {
        tops.create(Ptable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("error trying to create P table "+Ptable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("impossible",e);
        throw new RuntimeException(e);
      }

    String instance = connector.getInstance().getInstanceName();
    String zookeepers = connector.getInstance().getZooKeepers();
    String user = connector.whoami();

    Map<String,String> opt = new HashMap<>();
    opt.put("zookeeperHost", zookeepers);
    opt.put("instanceName", instance);
    opt.put("tableName", Atable);
    opt.put("username", user);
    opt.put("password", new String(password.getPassword()));
    opt.put("doWholeRow", "false");
    IteratorSetting itset = new IteratorSetting(2, RemoteSourceIterator.class, opt);
    List<IteratorSetting> list = new ArrayList<>();
    list.add(itset);
    list.add(new IteratorSetting(3, DevNull.class));
    try {
      //tops.attachIterator(Ptable, itset);
      long t1 = System.currentTimeMillis();
      // flush, block
      tops.compact(Ptable, null, null, list, true, true);
      long t2 = System.currentTimeMillis();
      log.info("Time for blocking compact() call to return: " + (t2 - t1) / 1000.0);
    } catch (AccumuloException e) {
      log.error("error trying to compact "+Ptable+" with TableMultIterator; is the iterator installed on the Accumulo server?", e);
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      log.error("error trying to compact "+Ptable+" with TableMultIterator", e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    }
  }

  public void testReadWriteBT(String Ptable, String Atable) {
    TableOperations tops = connector.tableOperations();
    if (!tops.exists(Ptable))
      try {
        tops.create(Ptable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("error trying to create P table "+Ptable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("impossible",e);
        throw new RuntimeException(e);
      }

    String instance = connector.getInstance().getInstanceName();
    String zookeepers = connector.getInstance().getZooKeepers();
    String user = connector.whoami();

    Map<String,String> opt = new HashMap<>();
    opt.put("zookeeperHost", zookeepers);
    opt.put("instanceName", instance);
    opt.put("tableName", Atable);
    opt.put("username", user);
    opt.put("password", new String(password.getPassword()));
    opt.put("doWholeRow", "false");
    IteratorSetting itset = new IteratorSetting(2, RemoteSourceIterator.class, opt);
    try {
      //tops.attachIterator(Ptable, itset);
      long t1 = System.currentTimeMillis();
      // flush, block
      tops.compact(Ptable, null, null, Collections.singletonList(itset), true, true);
      long t2 = System.currentTimeMillis();
      log.info("Time for blocking compact() call to return: " + (t2 - t1) / 1000.0);
    } catch (AccumuloException e) {
      log.error("error trying to compact "+Ptable+" with TableMultIterator; is the iterator installed on the Accumulo server?", e);
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      log.error("error trying to compact "+Ptable+" with TableMultIterator", e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    }
  }

  public void testReadOnlyBT(String Ptable, String Atable) {
    TableOperations tops = connector.tableOperations();
    if (!tops.exists(Ptable))
      try {
        tops.create(Ptable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("error trying to create P table "+Ptable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("impossible",e);
        throw new RuntimeException(e);
      }

    String instance = connector.getInstance().getInstanceName();
    String zookeepers = connector.getInstance().getZooKeepers();
    String user = connector.whoami();

    Map<String,String> opt = new HashMap<>();
    opt.put("zookeeperHost", zookeepers);
    opt.put("instanceName", instance);
    opt.put("tableName", Atable);
    opt.put("username", user);
    opt.put("password", new String(password.getPassword()));
    opt.put("doWholeRow", "false");
    IteratorSetting itset = new IteratorSetting(2, RemoteSourceIterator.class, opt);
    List<IteratorSetting> list = new ArrayList<>();
    list.add(itset);
    list.add(new IteratorSetting(3, DevNull.class));
    try {
      //tops.attachIterator(Ptable, itset);
      long t1 = System.currentTimeMillis();
      // flush, block
      tops.compact(Ptable, null, null, list, true, true);
      long t2 = System.currentTimeMillis();
      log.info("Time for blocking compact() call to return: " + (t2 - t1) / 1000.0);
    } catch (AccumuloException e) {
      log.error("error trying to compact "+Ptable+" with TableMultIterator; is the iterator installed on the Accumulo server?", e);
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      log.error("error trying to compact "+Ptable+" with TableMultIterator", e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    }
  }

  public void testReadWriteDot(String Ptable, String Atable, String BTtable) {
    TableOperations tops = connector.tableOperations();
    if (!tops.exists(Ptable))
      try {
        tops.create(Ptable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("error trying to create P table "+Ptable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("impossible",e);
        throw new RuntimeException(e);
      }

    String instance = connector.getInstance().getInstanceName();
    String zookeepers = connector.getInstance().getZooKeepers();
    String user = connector.whoami();

    Map<String,String> opt = new HashMap<>();
    opt.put("A.zookeeperHost", zookeepers);
    opt.put("A.instanceName", instance);
    opt.put("A.tableName", Atable);
    opt.put("A.username", user);
    opt.put("A.password", new String(password.getPassword()));
    opt.put("BT.zookeeperHost", zookeepers);
    opt.put("BT.instanceName", instance);
    opt.put("BT.tableName", BTtable);
    opt.put("BT.username", user);
    opt.put("BT.password", new String(password.getPassword()));
    IteratorSetting itset = new IteratorSetting(2, DotIterator.class, opt);
    try {
      //tops.attachIterator(Ptable, itset);
      long t1 = System.currentTimeMillis();
      // flush, block
      tops.compact(Ptable, null, null, Collections.singletonList(itset), true, true);
      long t2 = System.currentTimeMillis();
      log.info("Time for blocking compact() call to return: " + (t2 - t1) / 1000.0);
    } catch (AccumuloException e) {
      log.error("error trying to compact "+Ptable+" with TableMultIterator; is the iterator installed on the Accumulo server?", e);
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      log.error("error trying to compact "+Ptable+" with TableMultIterator", e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    }
  }

  public void testReadOnlyDot(String Ptable, String Atable, String BTtable) {
    TableOperations tops = connector.tableOperations();
    if (!tops.exists(Ptable))
      try {
        tops.create(Ptable);
      } catch (AccumuloException | AccumuloSecurityException e) {
        log.error("error trying to create P table "+Ptable, e);
        throw new RuntimeException(e);
      } catch (TableExistsException e) {
        log.error("impossible",e);
        throw new RuntimeException(e);
      }

    String instance = connector.getInstance().getInstanceName();
    String zookeepers = connector.getInstance().getZooKeepers();
    String user = connector.whoami();

    Map<String,String> opt = new HashMap<>();
    opt.put("A.zookeeperHost", zookeepers);
    opt.put("A.instanceName", instance);
    opt.put("A.tableName", Atable);
    opt.put("A.username", user);
    opt.put("A.password", new String(password.getPassword()));
    opt.put("BT.zookeeperHost", zookeepers);
    opt.put("BT.instanceName", instance);
    opt.put("BT.tableName", BTtable);
    opt.put("BT.username", user);
    opt.put("BT.password", new String(password.getPassword()));
    IteratorSetting itset = new IteratorSetting(2, DotIterator.class, opt);
    List<IteratorSetting> list = new ArrayList<>();
    list.add(itset);
    list.add(new IteratorSetting(3, DevNull.class));
    try {
      //tops.attachIterator(Ptable, itset);
      long t1 = System.currentTimeMillis();
      // flush, block
      tops.compact(Ptable, null, null, list, true, true);
      long t2 = System.currentTimeMillis();
      log.info("Time for blocking compact() call to return: " + (t2 - t1) / 1000.0);
    } catch (AccumuloException e) {
      log.error("error trying to compact "+Ptable+" with TableMultIterator; is the iterator installed on the Accumulo server?", e);
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      log.error("error trying to compact "+Ptable+" with TableMultIterator", e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    }
  }


}
