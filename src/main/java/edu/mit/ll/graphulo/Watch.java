package edu.mit.ll.graphulo;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.EnumMap;

/**
 * Spans and counters. For measuring performance.
 * Based on {@link org.apache.accumulo.core.util.StopWatch}.
 */
public class Watch<K extends Enum<K>> {
  private static final Logger log = LogManager.getLogger(Watch.class);

  static {
    log.info("Loading Watch");
  }

  static enum PerfSpan {
    Anext, ArowDecode, BTnext, BTrowDecode, Multiply
  }

//  /**
//   * Holds thread-local timing information.
//   */
//  static ThreadLocal<Watch<PerfSpan>> ThreadWatches =
//      new ThreadLocal<Watch<PerfSpan>>() {
//        @Override
//        protected Watch<PerfSpan> initialValue() {
//          return new Watch<>(PerfSpan.class);
//        }
//      };

  static final Watch<PerfSpan> instance = new Watch<>(PerfSpan.class);

  static class Stats {
    public long total=0, count=0, min=Long.MAX_VALUE, max=Long.MIN_VALUE;
  }

  EnumMap<K, Long> startTime;
  EnumMap<K, Stats> totalStats;

  public Watch(Class<K> s) {
    startTime = new EnumMap<K, Long>(s);
    totalStats = new EnumMap<K, Stats>(s);
  }

  public synchronized void start(K timer) {
    if (startTime.containsKey(timer)) {
      throw new IllegalStateException(timer + " already started");
    }
    startTime.put(timer, System.currentTimeMillis());
  }

  public synchronized void stopIfActive(K timer) {
    if (startTime.containsKey(timer))
      stop(timer);
  }

  public synchronized void stop(K timer) {

    Long st = startTime.get(timer);

    if (st == null) {
      throw new IllegalStateException(timer + " not started");
    }

    long dur = System.currentTimeMillis() - st;

    increment(timer, dur);

    startTime.remove(timer);
  }

  public synchronized void increment(K counter, long amount) {
    Stats stats = totalStats.get(counter);
    if (stats == null)
      stats = new Stats();

    stats.count++;
    stats.max = Math.max(stats.max, amount);
    stats.min = Math.min(stats.min, amount);
    stats.total = stats.total + amount;

    totalStats.put(counter, stats);
  }

  public synchronized void reset(K timer) {
    totalStats.remove(timer);
  }

  public synchronized void resetAll() {
    totalStats.clear();
  }

  public synchronized Stats get(K timer) {
    Stats stats = totalStats.get(timer);
    if (stats == null)
      stats = new Stats();
    return stats;
  }

//  public synchronized double getSecs(S timer) {
//    Long existingTime = totalStats.get(timer);
//    if (existingTime == null)
//      existingTime = 0L;
//    return existingTime / 1000.0;
//  }

  public synchronized void print() {
    System.out.println("THREAD: " + Thread.currentThread().getName());
//    log.info("THREAD: " + Thread.currentThread().getName());
    for (K timer : totalStats.keySet()) {
      Stats stats = get(timer);
      System.out.printf("%15s: %,7d tot %,10d cnt %,6d min %,6d max%n", timer.toString(),
          stats.total, stats.count, stats.min, stats.max);
//      log.info(String.format("%15s: %,7d tot %,10d cnt %,6d min %,6d max%n", timer.toString(),
//          stats.total, stats.count, stats.min, stats.max));
    }
  }


}
