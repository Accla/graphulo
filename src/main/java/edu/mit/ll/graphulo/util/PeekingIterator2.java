package edu.mit.ll.graphulo.util;



import java.util.Collections;
import java.util.Iterator;

/**
 * Caches two entries.
 */
public class PeekingIterator2<E> implements Iterator<E> {
//  private final Iterator<E> source;
  private PeekingIterator1<? extends E> pSecond, pFirst;
//  private E top;

  public PeekingIterator2(Iterator<? extends E> source) {
//    this.source = source;
    pSecond = new PeekingIterator1<>(source);
    pFirst = new PeekingIterator1<>(pSecond);
  }

  /** Iterate over a single value. */
  public PeekingIterator2(E top) {
    pSecond = PeekingIterator1.emptyIterator();
    pFirst = new PeekingIterator1<>(top);
  }

  /** Iterate over two values. */
  public PeekingIterator2(E first, E second) {
    pSecond = new PeekingIterator1<>(second);
    pFirst = new PeekingIterator1<>(pSecond, first);
  }

  @Override
  public boolean hasNext() {
    return pFirst.hasNext();
  }

  @Override
  public E next() {
    return pFirst.next();
  }

  @Override
  public void remove() {
    pFirst.remove();
  }

  public E peekFirst() {
    return pFirst.peek();
  }

  public E peekSecond() {
    return pSecond.peek();
  }


  private static final PeekingIterator2<?> EMPTY_ITERATOR
      = new PeekingIterator2<>(Collections.emptyIterator());

  @SuppressWarnings("unchecked")
  public static <T> PeekingIterator2<T> emptyIterator() {
    return (PeekingIterator2<T>) EMPTY_ITERATOR;
  }

}
