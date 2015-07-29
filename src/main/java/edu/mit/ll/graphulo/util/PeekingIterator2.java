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
