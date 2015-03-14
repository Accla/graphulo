package edu.mit.ll.graphulo.util;

import org.junit.runner.Runner;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

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
    List<Runner> runners = new LinkedList<Runner>();

    for (Class<?> klazz : classes) {
      runners.add(new KnownBugRunner(klazz));
    }

    return runners;
  }
}
