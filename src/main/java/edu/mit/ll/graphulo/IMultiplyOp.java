package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.util.*;

/**
 * Multiplication operation on 2 entries. Respects order for non-commutative operations.
 */
public interface IMultiplyOp {
  /** Useful for ignoring column visibility. */
  byte EMPTY_BYTES[] = new byte[0];

  /**
   * Multiplication operation on 2 entries from table AT and B that match on their row in the outer product.
   * @param Mrow Pointer to data for matching row. Do not modify.
   * @param ATcolF Pointer to data for AT column family. Do not modify.
   * @param ATcolQ Pointer to data for AT column qualifier. Do not modify.
   * @param BcolF Pointer to data for B column family. Do not modify.
   * @param BcolQ Pointer to data for B column qualifier. Do not modify.
   * @param ATval Pointer to data for AT value. Do not modify.
   * @param Bval Pointer to data for B value. Do not modify.
   * @param oc OutputCollector to write any results
   * @return Newly allocated Key and Value. Or return null to indicate the result is a zero.
   */
  Map.Entry<Key, Value> multiplyEntry(ByteSequence Mrow, ByteSequence ATcolF, ByteSequence ATcolQ,
                                      ByteSequence BcolF, ByteSequence BcolQ, Value ATval, Value Bval,
                                      OutputCollector oc);



}




// EXPERIMENTAL code below




/** Code called when a scan is initiated in Accumulo. */
class BottomIterator {

  /** A Buffer OutputCollector for sending to the client. */
  static class ClientSendBuffer implements OutputCollector {
    boolean acceptEntries = true;

    final int CAP = 5;
    Key[] keyBuf = new Key[CAP];
    Value[] valBuf = new Value[CAP];
    int pos = 0;

    /** Stop if error sending to client. */
    public boolean shouldStop() {
      return !acceptEntries;
    }

    /** Add a new entry to buffer to send to client, if no error so far sending to client. Do send if buffer fills.*/
    public void collect(Key k, Value v) {
      if (!acceptEntries)
        return;
      keyBuf[pos] = k;
      valBuf[pos] = v;
      if (++pos >= CAP) {
//        try { sendBufferToClient(keyBuf, valBuf); }
//        catch(IOException e) { acceptEntries = false; }
        pos = 0;
      }
    }

    /** Finished collecting; send remaining entries to client (if no error). */
    public void close() {
      if (!acceptEntries)
        return;
//      try { sendBufferToClient(keyBuf, valBuf); }
//      catch(IOException e) { acceptEntries = false; }
      acceptEntries = false;
    }
  }

  public void doScan(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    SortedKeyValueIterator<Key,Value> stack = null; //createIteratorStack();
    OutputCollector clientBuffer = new ClientSendBuffer(); // pass info for client connection

    if (stack instanceof EmitSKVI) {
      // use new OutputCollector paradigm
      ((EmitSKVI)stack).seekEmit(range, columnFamilies, inclusive, clientBuffer);
    } else {
      // backward compatible with old SKVIs
      try {
        stack.seek(range, columnFamilies, inclusive);
        while (stack.hasTop() && !clientBuffer.shouldStop()) {
          clientBuffer.collect(stack.getTopKey(), stack.getTopValue());
          stack.next();
        }
      } catch (IOException e) {
        // drop stack
      }
    }

  }

}

