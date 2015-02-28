package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.Pair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collection;

/**
 * Helper methods for testing.
 */
public class TestUtil {
    private static final Logger log = LogManager.getLogger(TestUtil.class);

    /**
     * Delete table if it exists and make it afresh.
     * Optionally insert entries into the new table.
     */
    public static void createTestTable(Connector conn, String tableName, Collection<Pair<Key,Value>> entriesToIngest) {
        if (conn.tableOperations().exists(tableName)) {
            try {
                conn.tableOperations().delete(tableName);
            } catch (AccumuloException | AccumuloSecurityException e) {
                throw new RuntimeException("cannot delete table "+tableName, e);
            } catch (TableNotFoundException e) {
                throw new RuntimeException("crazy timing bug", e);
            }
        }
        try {
            conn.tableOperations().create(tableName);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException("cannot create table " + tableName, e);
        } catch (TableExistsException e) {
            throw new RuntimeException("crazy timing bug", e);
        }

        if (entriesToIngest != null && !entriesToIngest.isEmpty()) {
            BatchWriterConfig config = new BatchWriterConfig();
            BatchWriter writer = null;
            try {
                writer = conn.createBatchWriter(tableName, config);
            } catch (TableNotFoundException e) {
                throw new RuntimeException("crazy timing bug", e);
            }
            Mutation m = null;
            try {
                for (Pair<Key, Value> pair : entriesToIngest) {
                    Key k = pair.getFirst();
                    Value v = pair.getSecond();
                    m = new Mutation(k.getRowData().getBackingArray());
                    m.put(k.getColumnFamilyData().getBackingArray(), k.getColumnQualifierData().getBackingArray(),
                            k.getColumnVisibilityParsed(), v.get()); // no ts? System.currentTimeMillis()
                    writer.addMutation(m);
                }
            } catch (MutationsRejectedException e) {
                throw new RuntimeException("rejected mutations; last one added is " + m, e);
            } finally {
                try {
                    writer.close();
                } catch (MutationsRejectedException e1) {
                    log.error("rejected mutations; last one added is " + m, e1);
                }
            }
        }
    }
}
