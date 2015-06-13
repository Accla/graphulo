package edu.mit.ll.graphulo.reducer;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;

/**
 * Tests for classes implementing {@link edu.mit.ll.graphulo.Reducer}.
 */
public class ReducerTest {

  @Test
  public void testEdgeBFSReducer() {
    EdgeBFSReducer r = new EdgeBFSReducer();
    r.init(Collections.singletonMap(EdgeBFSReducer.IN_COLUMN_PREFIX, "in|"), null);
    r.update(new Key("","","out|v1"), new Value());
    r.update(new Key("","","in|v2"), new Value());
    r.update(new Key("","","out|v1"), new Value());
    r.update(new Key("","","in|v3"), new Value());
    r.update(new Key("","","in|v3"), new Value());
    HashSet<String> e = new HashSet<>();
    e.add("v2");
    e.add("v3");
    Assert.assertEquals(e, r.get());
    EdgeBFSReducer r2 = new EdgeBFSReducer();
    r2.init(Collections.singletonMap(EdgeBFSReducer.IN_COLUMN_PREFIX, "in|"), null);
    r.update(new Key("","","in|v5"), new Value());
    r.combine(r2.get());
    e.add("v5");
    Assert.assertEquals(e, r.get());
  }

  @Test
  public void testSingleBFSReducer() {
    SingleBFSReducer r = new SingleBFSReducer();
    r.init(Collections.singletonMap(SingleBFSReducer.FIELD_SEP, "|"), null);
    r.update(new Key("v1"), new Value());
    r.update(new Key("v1|v2"), new Value());
    r.update(new Key("v1|v3"), new Value());
    r.update(new Key("v2|v2"), new Value());
    r.update(new Key("v2|v4"), new Value());
    HashSet<String> e = new HashSet<>();
    e.add("v2");
    e.add("v3");
    e.add("v4");
    Assert.assertEquals(e, r.get());
    SingleBFSReducer r2 = new SingleBFSReducer();
    r2.init(Collections.singletonMap(SingleBFSReducer.FIELD_SEP, "|"), null);
    r2.update(new Key("v1|v3"), new Value());
    r2.update(new Key("v2|v2"), new Value());
    r2.update(new Key("v8|v5"), new Value());
    r.combine(r2.get());
    e.add("v5");
    Assert.assertEquals(e, r.get());
  }

}
