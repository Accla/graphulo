package edu.mit.ll.graphulo;

import java.util.Iterator;

/**
 * Iterator that caches an entry. See {@link org.apache.accumulo.core.util.PeekingIterator}.
 */
public class PeekingIterator1<E> implements Iterator<E> {
  protected Iterator<E> source;
  protected E top;

  public PeekingIterator1(Iterator<E> source) {
    this.source = source;
    if (source.hasNext())
      top = source.next();
    else
      top = null;
  }

  /** Create a PeekingIterator1 with given starting element. */
  public PeekingIterator1(Iterator<E> source, E top) {
    this.source = source;
    this.top = top;
  }

  public E peek() {
    return top;
  }

  @Override
  public E next() {
    E lastPeeked = top;
    if (source.hasNext())
      top = source.next();
    else
      top = null;
    return lastPeeked;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasNext() {
    return top != null;
  }
}
