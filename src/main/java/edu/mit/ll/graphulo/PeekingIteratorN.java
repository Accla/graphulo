package edu.mit.ll.graphulo;

import org.apache.accumulo.core.util.PeekingIterator;

import java.lang.reflect.Array;
import java.util.Iterator;

/**
 * Cache N entries of an iterator.
 */
public class PeekingIteratorN<E> implements Iterator<E> {
  private PeekingIterator<E>[] ps;

  /**
   * Create an iterator that caches N items.
   * @param N How many items to cache?
   * @param source The source iterator.
   */
  @SuppressWarnings("unchecked")
  public PeekingIteratorN(int N, Iterator<E> source) {
    ps = (PeekingIterator<E>[]) Array.newInstance(PeekingIterator.class,N);
    for (int i = N-1; i >= 0; i--) {
      ps[i] = new PeekingIterator<E>(source);
      source = ps[i];
    }
  }

  @Override
  public boolean hasNext() {
    return ps[0].hasNext();
  }

  @Override
  public E next() {
    return ps[0].next();
  }

  @Override
  public void remove() {
    ps[0].remove();
  }

  /**
   * Look at the (n-1)st element in the iterator, counting from 1.
   * @throws ArrayIndexOutOfBoundsException If asked to peek beyond the originally given size.
   */
  public E peek(int n) {
    return ps[n-1].peek();
  }
}
