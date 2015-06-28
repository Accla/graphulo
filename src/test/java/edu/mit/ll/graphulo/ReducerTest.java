package edu.mit.ll.graphulo;

import edu.mit.ll.graphulo.reducer.EdgeBFSReducer;
import edu.mit.ll.graphulo.reducer.Reducer;
import edu.mit.ll.graphulo.reducer.SingleBFSReducer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Tests for classes implementing {@link Reducer}.
 */
public class ReducerTest {

  @Test
  public void testEdgeBFSReducer() {
    EdgeBFSReducer r = new EdgeBFSReducer();
    r.init(Collections.singletonMap(EdgeBFSReducer.IN_COLUMN_PREFIX, "in|,"), null);
    r.update(new Key("","","out|v1"), new Value());
    r.update(new Key("","","in|v2"), new Value());
    r.update(new Key("","","out|v1"), new Value());
    r.update(new Key("","","in|v3"), new Value());
    r.update(new Key("","","in|v3"), new Value());
    HashSet<String> e = new HashSet<>();
    e.add("v2");
    e.add("v3");
    Assert.assertEquals(e, r.getForClient());
    EdgeBFSReducer r2 = new EdgeBFSReducer();
    r2.init(Collections.singletonMap(EdgeBFSReducer.IN_COLUMN_PREFIX, "in|,"), null);
    r.update(new Key("","","in|v5"), new Value());
    r.combine(r2.getForClient());
    e.add("v5");
    Assert.assertEquals(e, r.getForClient());
  }

  @Test
  public void testSingleBFSReducer() {
    SingleBFSReducer r = new SingleBFSReducer();
    r.init(Collections.singletonMap(SingleBFSReducer.EDGE_SEP, "|"), null);
    r.update(new Key("v1", "", "", 3), new Value());
    r.update(new Key("v1|v2", "", "", 2), new Value()); // must be even timestamp
    r.update(new Key("v1|v3", "", "", 2), new Value());
    r.update(new Key("v2|v2", "", "", 2), new Value());
    r.update(new Key("v2|v4", "", "", 2), new Value());
    HashSet<String> e = new HashSet<>();
    e.add("v2");
    e.add("v3");
    e.add("v4");
    Assert.assertEquals(e, r.getForClient());
    SingleBFSReducer r2 = new SingleBFSReducer();
    Map<String,String> map = new HashMap<>();
    map.put(SingleBFSReducer.EDGE_SEP, "|");
//    map.put(SingleTransposeIterator.EDGESEP, "");
//    map.put(SingleTransposeIterator.NEG_ONE_IN_DEG, Boolean.toString(false /*copyOutDegrees*/));
//    map.put(SingleTransposeIterator.DEGCOL, "");
    r2.init(map, null);
    r2.update(new Key("v1|v3", "", "", 2), new Value());
    r2.update(new Key("v2|v2", "", "", 2), new Value());
    r2.update(new Key("v8|v5", "", "", 2), new Value());
    r.combine(r2.getForClient());
    e.add("v5");
    Assert.assertEquals(e, r.getForClient());
  }

}
