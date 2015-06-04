package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.mult.LongEWiseX;
import edu.mit.ll.graphulo.mult.LongMultiply;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

/**
 * Created by ja26144 on 6/4/15.
 */
public class SCCGraphulo extends Graphulo {

  public SCCGraphulo(Connector connector, PasswordToken password) {
    super(connector, password);
  }

  public void SCC(String Atable, String Rtable, Long rowCount, int numEntriesCheckpoint, boolean trace) throws AccumuloSecurityException, AccumuloException,
      TableNotFoundException {
    if (Atable == null || Atable.isEmpty()) {
      throw new IllegalArgumentException("Please specify table A. Given: " + Atable);
    }
    if (Rtable == null || Rtable.isEmpty()) {
      throw new IllegalArgumentException("Please specify table AT. Given: " + Rtable);
    }
    if (rowCount < 1) {
      throw new IllegalArgumentException("Table too small.");
    }

    TableOperations tops = connector.tableOperations();
    String tA = Atable;
    String tAC = Atable + " Copy";
    String tAT = Atable + " Transpose";
    String tR = Rtable + " Temporary";
    String tRT = Rtable + " Transpose";
    String tRf = Rtable;

    AdjBFS(tA, null, 1, tR, tAT, null, "", true, 0, 214483647, null, trace);
    for (int n = 1; n < rowCount; n++) {
      if (n % 2 == 1) {
        tops.delete(tAC);
        TableMult(tAT, tA, tR, tAC, LongMultiply.class, null, null, null, null, numEntriesCheckpoint, trace);
      } else {
        tops.delete(tAT);
        TableMult(tAC, tA, tR, tAT, LongMultiply.class, null, null, null, null, numEntriesCheckpoint, trace);
      }
    }

    AdjBFS(tR, null, 1, null, tRT, null, "", true, 0, 214483647, null, trace);

    SpEWiseX(tR, tRT, tRf, null, LongEWiseX.class, null, null, null, null, numEntriesCheckpoint, trace);

  }
}
