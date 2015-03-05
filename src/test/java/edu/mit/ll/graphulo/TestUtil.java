package edu.mit.ll.graphulo;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;

/**
 * Helper methods for testing.
 */
public class TestUtil {
    private static final Logger log = LogManager.getLogger(TestUtil.class);


    public static void createTestTable(Connector conn, String tableName) {
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
    }

    public static void createTestTable(Connector conn, String tableName, SortedSet<Text> splits) {
        createTestTable(conn, tableName);

        if (splits != null && !splits.isEmpty())
            try {
                conn.tableOperations().addSplits(tableName, splits);
            } catch (TableNotFoundException e) {
                throw new RuntimeException("crazy timing bug", e);
            } catch (AccumuloException | AccumuloSecurityException e) {
                throw new RuntimeException("failed to create table splits on "+tableName, e);
            }
    }

    /**
     * Delete table if it exists and make it afresh.
     * Optionally insert entries into the new table.
     */
    public static void createTestTable(Connector conn, String tableName, SortedSet<Text> splits, Map<Key,Value> entriesToIngest) {
        createTestTable(conn, tableName, splits);

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
                for (Map.Entry<Key, Value> pair : entriesToIngest.entrySet()) {
                    Key k = pair.getKey();
                    Value v = pair.getValue();
                    m = new Mutation(k.getRowData().getBackingArray());
                    if (k.isDeleted())
                        m.putDelete(k.getColumnFamilyData().getBackingArray(), k.getColumnQualifierData().getBackingArray(),
                                k.getColumnVisibilityParsed()); // no ts? System.currentTimeMillis()
                    else
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

//    public static Collection<Map.Entry<Key,Value>> pairsToEntries(Collection<Pair<Key,Value>> entries) {
//        Collection<Map.Entry<Key,Value>> newset = new HashSet<>(entries.size());
//        for (Pair<Key, Value> entry : entries) {
//            newset.add(entry.toMapEntry());
//        }
//        return newset;
//    }

    /** Compare only the row, column family and column qualifier. */
    static class KeyRowColFColQComparator implements Comparator<Key> {
        @Override
        public int compare(Key k1, Key k2) {
            return k1.compareTo(k2, PartialKey.ROW_COLFAM_COLQUAL);
        }
    }
    static final KeyRowColFColQComparator COMPARE_KEY_TO_COLQ = new KeyRowColFColQComparator();

//    /**
//     * Assert two collections of Keys are equal up to row, colummn family, column qulalifier.
//     */
//    public static void assertEqualEntriesRowColFColQ(Set<Map.Entry<Key, Value>> expect, Set<Map.Entry<Key, Value>> actual) {
//        if (expect == null && actual == null)
//            return;
//        if (expect == null
//                || actual == null
//                || expect.size() != actual.size()) {
//            failWithNiceMsg("different sizes;", expect, actual);
//            return;
//        }
//        Map<Key,Value> map = new TreeMap<>(COMPARE_KEY_TO_COLQ);
//        //expect.
//        SortedSet<Map.Entry<Key,Value>> a1 = new TreeSet<>(expect), a2 = new TreeSet<>(actual);
//        Iterator<Map.Entry<Key, Value>> i1 = a1.iterator(), i2 = a2.iterator();
//        while (i1.hasNext()) {
//            Map.Entry<Key,Value> e1 = i1.next(), e2 = i2.next();
//            if (!e1.getKey().equals(e2.getKey(), PartialKey.ROW_COLFAM_COLQUAL) // *
//                || !e1.getValue().equals(e2.getValue()))
//                failWithNiceMsg("expect "+e1+" does not match actual "+e2+";", expect, actual);
//        }
//    }
//
//    /** Adapted from {@link org.junit.Assert} */
//    private static void failWithNiceMsg(String message, Object expected, Object actual) {
//        String formatted = "";
//        if (message != null && !message.equals("")) {
//            formatted = message + " ";
//        }
//        String expectedString = String.valueOf(expected);
//        String actualString = String.valueOf(actual);
//        String msg;
//        if (expectedString.equals(actualString)) {
//            msg = formatted + "expected: "
//                    + formatClassAndValue(expected, expectedString)
//                    + " but was: " + formatClassAndValue(actual, actualString);
//        } else {
//            msg = formatted + "expected:<" + expectedString + "> but was:<"
//                    + actualString + ">";
//        }
//        Assert.fail(msg);
//    }
//
//    /** Adapted from {@link org.junit.Assert} */
//    private static String formatClassAndValue(Object value, String valueString) {
//        String className = value == null ? "null" : value.getClass().getName();
//        return className + "<" + valueString + ">";
//    }

}
