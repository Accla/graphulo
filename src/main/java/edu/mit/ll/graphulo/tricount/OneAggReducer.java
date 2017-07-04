package edu.mit.ll.graphulo.tricount;

import edu.mit.ll.graphulo.reducer.ReducerSerializable;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.UIntegerLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 *
 */
public final class OneAggReducer extends ReducerSerializable<Long> {
  private static final Logger log = LogManager.getLogger(OneAggReducer.class);

  private long triangles = 0L;


  @Override
  public void init(Map<String, String> options, IteratorEnvironment env) {
  }

  @Override
  public void reset() throws IOException {
    triangles = 0L;
  }

  @Override
  public void update(Key k, Value v) {
    if( v.equals(GraphuloUtil.VALUE_ONE_STRING) )
      triangles++;
  }

  @Override
  public void combine(Long another) {
    triangles += another;
  }

  @Override
  public boolean hasTopForClient() {
    return triangles != 0;
  }

  @Override
  public Long getSerializableForClient() {
    return triangles;
  }
}
