package edu.mit.ll.graphulo;



import java.util.Iterator;

/**
 * Caches two entries.
 */
public class PeekingIterator2<E> implements Iterator<E> {
//  private final Iterator<E> source;
  private PeekingIterator1<E> pSecond, pFirst;
//  private E top;

  public PeekingIterator2(Iterator<E> source) {
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

}
