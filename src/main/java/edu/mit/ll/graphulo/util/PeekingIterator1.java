package edu.mit.ll.graphulo.util;

import java.util.Collections;
import java.util.Iterator;

/**
 * Iterator that caches an entry. See {@link org.apache.accumulo.core.util.PeekingIterator}.
 */
public class PeekingIterator1<E> implements Iterator<E> {
  protected Iterator<? extends E> source;
  protected E top;

  public PeekingIterator1(Iterator<? extends E> source) {
    this.source = source == null ? Collections.<E>emptyIterator() : source;
    if (!this.source.hasNext())
      top = null;
    else
      top = this.source.next();
  }

  /** Create a PeekingIterator1 with given starting element. */
  public PeekingIterator1(Iterator<? extends E> source, E top) {
    this.source = source == null ? Collections.<E>emptyIterator() : source;
    this.top = top;
  }

  /** Create a PeekingIterator1 with a single element. */
  public PeekingIterator1(E top) {
    this.source = Collections.<E>emptyIterator();
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

  private static final PeekingIterator1<?> EMPTY_ITERATOR
      = new PeekingIterator1<>(Collections.emptyIterator());

  @SuppressWarnings("unchecked")
  public static <T> PeekingIterator1<T> emptyIterator() {
    return (PeekingIterator1<T>) EMPTY_ITERATOR;
  }
}
