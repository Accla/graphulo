package edu.mit.ll.graphulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import java.util.*;

/**
 * Pass in a target set of ranges, such as { [b,g], (j,m], [q,+inf) }. Infinite range by default.
 * Iterate over the ones that overlap with a given seek range, such as (-inf,k].
 * Results is iteration returning [b,g], (j,k].
 */
public class RangeSet {
  private static final SortedSet<Range> INF_RANGE_SET = Collections.unmodifiableSortedSet(new TreeSet<>(Collections.singleton(new Range())));

  private SortedSet<Range> targetRanges = INF_RANGE_SET;

  /** Set the target ranges that we will iterator over, before applying a "seek range mask".
   * Merges overlapping ranges together.  Infinite range by default. */
  public void setTargetRanges(Collection<Range> ranges) {
    targetRanges = new TreeSet<>(Range.mergeOverlapping(new TreeSet<>(ranges)));
  }

  /** Iterate over target ranges in order, masked by seekRange.
   * Only iterates over target ranges that intersect the seekRange. */
  public PeekingIterator1<Range> iteratorWithRangeMask(Range seekRange) {
    if (seekRange.isInfiniteStartKey() && seekRange.isInfiniteStopKey())
      return new PeekingIterator1<>(targetRanges.iterator());
    else if (seekRange.isInfiniteStartKey())
      return new RangeSetIter(targetRanges.iterator(), seekRange);
    else {
      // find first range whose end key >= the start key of seekRange
      PeekingIterator1<Range> pi = getFirstRangeStarting(seekRange, targetRanges);
      if (seekRange.isInfiniteStopKey())
        return pi;
      else
        return new RangeSetIter(pi, seekRange);
    }
  }

  /**
   * Advance to the first subset range whose end key >= the seek start key.
   */
  public static PeekingIterator1<Range> getFirstRangeStarting(Range seekRange, SortedSet<Range> rowRanges) {
    PeekingIterator1<Range> iter = new PeekingIterator1<>(rowRanges.iterator());
    Key seekRangeStart = seekRange.getStartKey();
    if (seekRangeStart != null)
      while (iter.hasNext() && !iter.peek().isInfiniteStopKey()
          && (
          iter.peek().getEndKey().compareTo(seekRangeStart) < 0 ||
              (iter.peek().getEndKey().equals(seekRangeStart) && !seekRange.isEndKeyInclusive())
      ))
        iter.next();
    return iter;
  }

  private static class RangeSetIter extends PeekingIterator1<Range> {
    private final Range seekRange;

    RangeSetIter(Iterator<Range> sourceIter, Range seekRange) {
      super(sourceIter);
      this.seekRange = seekRange;
      super.top = super.top == null ? null : seekRange.clip(super.top, true);
    }

    RangeSetIter(PeekingIterator1<Range> sourceIter, Range seekRange) {
      super(sourceIter.source, sourceIter.top);
      this.seekRange = seekRange;
      super.top = super.top == null ? null : seekRange.clip(super.top, true);
    }

    @Override
    public Range next() {
      Range r = super.next();
      super.top = super.top == null ? null : seekRange.clip(super.top, true);
      return r;
    }
  }

}
