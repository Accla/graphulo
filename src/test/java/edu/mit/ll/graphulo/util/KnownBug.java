package edu.mit.ll.graphulo.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used to mark a JUnit @Test method as expected to fail due to a known bug.
 * If it fails, the {@link KnownBugRunner} will convert the failure to an assumption failure.
 * Remove the annotation once the known bug is fixed.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface KnownBug {
  /**
   * Description of the bug.
   */
  String value();
}

