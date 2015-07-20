package edu.mit.ll.graphulo.reducer;

import org.apache.commons.lang.SerializationUtils;

import java.io.Serializable;

/**
 * A Reducer for a Serializable object.
 * Has helper methods to serialize and deserialize the Reducer byte[] to {@link E}.
 */
public abstract class ReducerSerializable<E extends Serializable> implements Reducer {

  /** Implement this instead of {@link #combine(byte[])}. */
  public abstract void combine(E another);

  /** Implement this instead of {@link #getForClient()}. */
  public abstract E getSerializableForClient();


  @SuppressWarnings("unchecked")
  @Override
  public final void combine(byte[] another) {
    combine((E) SerializationUtils.deserialize(another));
  }

  @Override
  public final byte[] getForClient() {
    return SerializationUtils.serialize(getSerializableForClient());
  }
}
