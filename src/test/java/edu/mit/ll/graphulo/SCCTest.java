package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.util.AccumuloTestBase;
import edu.mit.ll.graphulo.util.TestUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * Created by ja26144 on 6/3/15.
 */
public class SCCTest extends AccumuloTestBase {

    private static final Logger log = LogManager.getLogger(SCCTest.class);

    /**
     *      vBig
     *   /   ^    \
     *  v    v     v
     * v0--->v1--->v2--v
     *  ^--<------<----/
     */
    @Test
    public void SCCTest() throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        Connector conn = tester.getConnector();
        final String tA, tAC, tAT, tR, tRT, tRf;
        {
            String[] names = getUniqueNames(2);
            tA = names[0];
            tR = names[1];
        }
        Map<Key,Value> expect = new TreeMap<Key, Value>(TestUtil.COMPARE_KEY_TO_COLQ),
                actual = new TreeMap<Key, Value>(TestUtil.COMPARE_KEY_TO_COLQ),
                expectTranspose = new TreeMap<Key, Value>(TestUtil.COMPARE_KEY_TO_COLQ),
                actualTranspose = new TreeMap<Key, Value>(TestUtil.COMPARE_KEY_TO_COLQ);

        {
            Map<Key, Value> input = new HashMap<>();
            input.put(new Key("v0", "", "v1"), new Value("1".getBytes()));
            input.put(new Key("v1", "", "v2"), new Value("1".getBytes()));
            input.put(new Key("v2", "", "v3"), new Value("1".getBytes()));
            input.put(new Key("vBig", "", "v1"), new Value("1".getBytes()));
            input.put(new Key("vBig", "", "v2"), new Value("1".getBytes()));
            input.put(new Key("vBig", "", "v3"), new Value("1".getBytes()));

            Map<Key, Value> e = new HashMap<>();
            e.put(new Key("v0", "", "v1"), new Value("1".getBytes()));
            e.put(new Key("v0", "", "v2"), new Value("1".getBytes()));
            e.put(new Key("v0", "", "v0"), new Value("1".getBytes()));

            e.put(new Key("v1", "", "v2"), new Value("1".getBytes()));
            e.put(new Key("v1", "", "v0"), new Value("1".getBytes()));
            e.put(new Key("v1", "", "v1"), new Value("1".getBytes()));

            e.put(new Key("v2", "", "v0"), new Value("1".getBytes()));
            e.put(new Key("v2", "", "v1"), new Value("1".getBytes()));
            e.put(new Key("v2", "", "v2"), new Value("1".getBytes()));


            expect.putAll(e);
            expectTranspose.putAll(TestUtil.transposeMap(e));
            TestUtil.createTestTable(conn, tA, null, expect);
        }

        Graphulo graphulo = new Graphulo(conn, tester.getPassword());




    }


}
