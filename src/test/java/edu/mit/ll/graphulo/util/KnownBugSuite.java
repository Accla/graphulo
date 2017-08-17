package edu.mit.ll.graphulo.util;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Suite to run everything with a KnownBugRunner
 */
public class KnownBugSuite extends Suite {

  // copied from Suite
  private static Class<?>[] getAnnotatedClasses(Class<?> klass) throws InitializationError {
    Suite.SuiteClasses annotation = klass.getAnnotation(Suite.SuiteClasses.class);
    if (annotation == null) {
      throw new InitializationError(String.format("class '%s' must have a SuiteClasses annotation", klass.getName()));
    }
    return annotation.value();
  }

  // copied from Suite
  public KnownBugSuite(Class<?> klass, RunnerBuilder builder) throws InitializationError {
    super(klass, getRunners(getAnnotatedClasses(klass)));
  }

  public static List<Runner> getRunners(Class<?>[] classes) throws InitializationError {
    List<Runner> runners = new LinkedList<>();

    for (Class<?> klazz : classes) {
      runners.add(new KnownBugRunner(klazz));
    }

    return runners;
  }

  @Override
  public void run(RunNotifier notifier) {
//    psout.println("Adding ConsoleRunListener");
    RunListener crl = new KnownBugSuite.ConsoleRunListener();
    notifier.addListener(crl);
    super.run(notifier);
    notifier.removeListener(crl);
  }

  static class ConsoleRunListener extends RunListener {
    PrintStream psOut = new PrintStream(new FileOutputStream(FileDescriptor.out));

    private long t = System.currentTimeMillis();
    private static final long NOTIFY_DUR = 10000l; // Show progress no sooner than 10 seconds apart.
    private int testFinishedCount = 0;

    @Override
    public void testStarted(Description description) throws Exception {
//      psOut.println("Starting " + description.toString());
    }

    @Override
    public void testFinished(Description description) throws Exception {
      testFinishedCount++;
      long dur = System.currentTimeMillis() - t;
      if (dur > NOTIFY_DUR) {
        psOut.printf("Finished %2d tests in the last %4.1f seconds%n", testFinishedCount, dur/1000.0);
        testFinishedCount = 0;
        t = System.currentTimeMillis();
      }
//      psOut.println("Finished " + description.toString());
    }

    @Override
    public void testFailure(Failure failure) throws Exception {
//      psOut.print('F') ; psOut.println();
      testFinishedCount = 0;
      t = System.currentTimeMillis();
      psOut.println("Failure " + failure.toString() + System.lineSeparator() + failure.getTrace());
    }

    @Override
    public void testAssumptionFailure(Failure failure) {
      testFinishedCount = 0;
      t = System.currentTimeMillis();
      if (failure.getMessage().startsWith("KnownBug Test Failure: "))
//        psOut.print('KMER');
        psOut.println("Test failed as expected, due to a known bug: "+failure.toString());
      else {
//        psOut.print('A') ; psOut.println();
        psOut.println("Assumption failure " + failure.toString());
      }
    }

    @Override
    public void testIgnored(Description description) throws Exception {
      testFinishedCount = 0;
      t = System.currentTimeMillis();
//      psOut.print('I');
      psOut.println("Ignoring " + description.toString());
    }
  }

}
