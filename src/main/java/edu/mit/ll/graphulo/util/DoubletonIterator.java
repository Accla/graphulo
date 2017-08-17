package edu.mit.ll.graphulo.util;

import java.util.Iterator;

/**
 * Iterator over two elements. Can be reset and reused.
 */
public class DoubletonIterator<E> implements Iterator<E> {

  private E e1, e2;
  private int pos = 1;

  public DoubletonIterator() {
  }

  public DoubletonIterator(E e1, E e2) {
    this.e1 = e1;
    this.e2 = e2;
  }

  public void reset() {
    pos = 1;
  }

  public void reuseAndReset(E e1, E e2) {
    this.e1 = e1;
    this.e2 = e2;
    pos = 1;
  }

  @Override
  public boolean hasNext() {
    return pos < 3;
  }

  @Override
  public E next() {
    E ret;
    switch(pos) {
      case 1: ret = e1; break;
      case 2: ret = e2; break;
      default: return null;
    }
    pos++;
    return ret;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove is not supported");
  }
}
