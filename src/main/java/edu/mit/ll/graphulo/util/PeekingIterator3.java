package edu.mit.ll.graphulo.util;



import java.util.Iterator;

/**
 * Caches three entries.
 */
public class PeekingIterator3<E> implements Iterator<E> {
//  private final Iterator<E> source;
  private PeekingIterator1<? extends E> pThird, pSecond, pFirst;
//  private E top;

  public PeekingIterator3(Iterator<? extends E> source) {
//    this.source = source;
    pThird = new PeekingIterator1<>(source);
    pSecond = new PeekingIterator1<>(pThird);
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

  public E peekThird() { return pThird.peek(); }

}
