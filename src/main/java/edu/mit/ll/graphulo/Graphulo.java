package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;

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
  public void TableMult(String Ptable,
                        String Atable, String BTtable,
                        Class<? extends IMultiplyOp> multOp, Class<? extends Combiner> sumOp,
                        Collection<Range> rowFilter,
                        Collection<IteratorSetting.Column> colFilter,
                        String Ctable, String Rtable, boolean wait) {
    TableMult(Ptable, Atable, BTtable, multOp, sumOp, rowFilter, colFilter, Ctable, Rtable, wait);
  }

  public void TableMult(String Ptable,
                        String Atable, String BTtable,
                        Class<? extends IMultiplyOp> multOp, Class<? extends Combiner> sumOp,
                        Collection<Range> rowFilter,
                        Collection<IteratorSetting.Column> colFilter,
                        String Ctable, String Rtable, boolean wait,
                        boolean trace) {
    if (Ptable == null || Ptable.isEmpty())
      throw new IllegalArgumentException("Please specify table P. Given: "+Ptable);
    if (Atable == null || Atable.isEmpty())
      throw new IllegalArgumentException("Please specify table A. Given: "+Atable);
    if (BTtable == null || BTtable.isEmpty())
      throw new IllegalArgumentException("Please specify table BT. Given: "+BTtable);
//    if (Rtable == null || Rtable.isEmpty())
//      throw new IllegalArgumentException("Please specify table R. Given: "+Rtable);
    if (Atable.equals(Ptable))
      log.warn("Untested combination: Atable=Ptable="+Atable);
    if (BTtable.equals(Ptable))
      log.warn("Untested combination: BTtable=Ptable="+BTtable);

    if (rowFilter != null && !rowFilter.isEmpty())
      throw new UnsupportedOperationException("rowFilter is not yet implemented; given: "+rowFilter);
    if (colFilter != null && !colFilter.isEmpty())
      throw new UnsupportedOperationException("colFilter is not yet implemented; given: "+colFilter);
//    if (multOp == null || !multOp.equals(BigDecimalMultiply.class))
//      throw new UnsupportedOperationException("only supported multOp is BigDecimalMultiply, but given: "+multOp);
//    if (sumOp == null || !sumOp.equals(BigDecimalCombiner.BigDecimalSummingCombiner.class))
//      throw new UnsupportedOperationException("only supported sumOp is BigDecimalSummingCombiner, but given: "+multOp);

    TableOperations tops = connector.tableOperations();
    if (!tops.exists(Atable))
      throw new IllegalArgumentException("Table A does not exist. Given: "+Atable);
    if (!tops.exists(BTtable))
      throw new IllegalArgumentException("Table BT does not exist. Given: "+BTtable);

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
//    opt.put("trace","true"); // enable distributed tracer

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
//    opt.put("BT.doClientSideIterators", "true");

    if (Rtable != null && !Rtable.isEmpty() && !Ptable.equals(Rtable)) {
      opt.put("R.zookeeperHost", zookeepers);
      opt.put("R.instanceName", instance);
      opt.put("R.tableName", Rtable);
      opt.put("R.username", user);
      opt.put("R.password", new String(password.getPassword()));
    }
    // okay if C==R
    if (Ctable != null && !Ctable.isEmpty() && !Ctable.equals(Ptable)) {
      opt.put("C.zookeeperHost", zookeepers);
      opt.put("C.instanceName", instance);
      opt.put("C.tableName", Ctable);
      opt.put("C.username", user);
      opt.put("C.password", new String(password.getPassword()));
    }

    // attach combiner on Rtable
    Map<String, String> optSum = new HashMap<>();
    optSum.put("all", "true");
    IteratorSetting iSum = new IteratorSetting(19,BigDecimalCombiner.BigDecimalSummingCombiner.class, optSum);
    try {
      tops.attachIterator(Rtable, iSum);
    } catch (AccumuloSecurityException | AccumuloException e) {
      log.error("error trying to add BigDecimalSummingCombiner to " + Rtable, e);
      throw new RuntimeException(e);
    } catch (TableNotFoundException e) {
      log.error("impossible", e);
      throw new RuntimeException(e);
    }

    // TODO P2: Assign priority and name dynamically, checking for conflicts.
    IteratorSetting itset = new IteratorSetting(2, TableMultIterator.class, opt);
    try {
      //tops.attachIterator(Ptable, itset);
      long t1 = System.currentTimeMillis();
      // flush, block
      tops.compact(Ptable, null, null, Collections.singletonList(itset), true, wait);
      long t2 = System.currentTimeMillis();
      log.info("Time for blocking compact() call to return: "+(t2-t1)/1000.0);

//      tops.flush(Rtable,null,null,true);

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

      // cancel compaction if compaction errors,
      // or else Accumulo will continue restarting compaction and erroring
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
