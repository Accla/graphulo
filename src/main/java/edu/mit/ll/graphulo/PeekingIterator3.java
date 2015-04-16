package edu.mit.ll.graphulo;

import org.apache.accumulo.core.util.PeekingIterator;

import java.util.Iterator;

/**
 * Cache two entries.
 */
public class PeekingIterator3<E> implements Iterator<E> {
//  private final Iterator<E> source;
  private PeekingIterator<E> pThird, pSecond, pFirst;
//  private E top;

  public PeekingIterator3(Iterator<E> source) {
//    this.source = source;
    pThird = new PeekingIterator<>(source);
    pSecond = new PeekingIterator<>(pThird);
    pFirst = new PeekingIterator<>(pSecond);
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
