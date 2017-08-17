package edu.mit.ll.graphulo.skvi;

import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.WrappingIterator;

import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Decode a double value from its byte representation to a String representation
 */
public class DoubleDecodeIterator extends WrappingIterator implements OptionDescriber {

  private static final Lexicoder<Double> LEXDOUBLE = new DoubleLexicoder();

  @Override
  public Value getTopValue() {
    Value v = super.getTopValue();
    v.set(Double.toString(LEXDOUBLE.decode(super.getTopValue().get())).getBytes(UTF_8));
    return v;
  }

  @Override
  public IteratorOptions describeOptions() {
    return new IteratorOptions("DoubleDecodeIterator",
        "Decode a double value from its byte representation to a String representation",
        null, null);
  }

  @Override
  public boolean validateOptions(Map<String, String> options) {
    return true;
  }
}
