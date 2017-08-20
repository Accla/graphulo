package edu.mit.ll.graphulo.skvi;

import edu.mit.ll.graphulo.tricount.IntegerEmptyLexicoder;
import edu.mit.ll.graphulo.util.IntegerOneLexicoder;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.client.lexicoder.UIntegerLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.ValueFormatException;

import java.io.IOException;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A TypedValueCombiner that translates each Value to a Long before reducing, then encodes the reduced Long back to a Value.
 *
 * Subclasses must implement a typedReduce method: {@code public Long typedReduce(Key key, Iterator<Long> iter);}
 *
 * This typedReduce method will be passed the most recent Key and an iterator over the Values (translated to Longs) for all non-deleted versions of that Key.
 *
 * A required option for this Combiner is "type" which indicates which type of Encoder to use to encode and decode Longs into Values. Supported types are
 * VARNUM, LONG, and STRING which indicate the VarNumEncoder, LongEncoder, and StringEncoder respectively.
 */
public abstract class IntCombiner extends TypedValueCombiner<Integer> {
  public static final Encoder<Integer> BYTE_ENCODER = new IntegerLexicoder(); // attempt 1.6 compat
  public static final Encoder<Integer> UBYTE_ENCODER = new UIntegerLexicoder(); // attempt 1.6 compat
  public static final Encoder<Integer> BYTE_ONE_ENCODER = new IntegerOneLexicoder(); // attempt 1.6 compat
  public static final Encoder<Integer> BYTE_EMPTY_ENCODER = new IntegerEmptyLexicoder(); // attempt 1.6 compat
  public static final Encoder<Integer> STRING_ENCODER = new StringEncoder();

  protected static final String TYPE = "type";
  protected static final String CLASS_PREFIX = "class:";

  public enum Type {
    /**
     * indicates a variable-length encoding of an Integer using {@link org.apache.accumulo.core.client.lexicoder.IntegerLexicoder}
     */
    BYTE,
    /**
     * indicates a variable-length encoding of an Integer using {@link org.apache.accumulo.core.client.lexicoder.IntegerLexicoder}
     */
    UBYTE,
    /**
     * indicates a variable-length encoding of an Integer using {@link org.apache.accumulo.core.client.lexicoder.IntegerLexicoder}
     * except that it checks for the presence of "1" and treats empty values as 1.
     */
    BYTE_ONE,
    /**
     * indicates a variable-length encoding of an Integer using {@link org.apache.accumulo.core.client.lexicoder.IntegerLexicoder}
     * except that it treats empty values as "1".
     */
    BYTE_EMPTY,
    /**
     * indicates a string representation of an Integer using {@link IntCombiner.StringEncoder}
     */
    STRING
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    setEncoder(options);
  }

  private void setEncoder(Map<String,String> options) {
    String type = options.get(TYPE);
    if (type == null)
      throw new IllegalArgumentException("no type specified");
    if (type.startsWith(CLASS_PREFIX)) {
      setEncoder(type.substring(CLASS_PREFIX.length()));
      testEncoder(42);
    } else {
      switch (Type.valueOf(type)) {
        case BYTE:
          setEncoder(BYTE_ENCODER);
          return;
        case UBYTE:
          setEncoder(UBYTE_ENCODER);
          return;
        case BYTE_ONE:
          setEncoder(BYTE_ONE_ENCODER);
          return;
        case BYTE_EMPTY:
          setEncoder(BYTE_EMPTY_ENCODER);
          return;
        case STRING:
          setEncoder(STRING_ENCODER);
          return;
        default:
          throw new IllegalArgumentException();
      }
    }
  }

  @Override
  public IteratorOptions describeOptions() {
    IteratorOptions io = super.describeOptions();
    io.setName("IntegerCombiner");
    io.setDescription("IntegerCombiner interprets Values as Integers in byte-wise or string encoding before combining");
    io.addNamedOption(TYPE, "<BYTE|STRING|fullClassName>");
    return io;
  }

  @Override
  public boolean validateOptions(Map<String,String> options) {
    if (!super.validateOptions(options))
      return false;
    try {
      setEncoder(options);
    } catch (Exception e) {
      throw new IllegalArgumentException("bad encoder option", e);
    }
    return true;
  }


  /**
   * An Encoder that uses a String representation of Longs. It uses Long.toString and Long.parseLong for encoding and decoding.
   */
  public static class StringEncoder implements Encoder<Integer> { // 1.6 compat
    @Override
    public byte[] encode(Integer v) {
      return Integer.toString(v).getBytes(UTF_8);
    }

    @Override
    public Integer decode(byte[] b) {
      // This concrete implementation is provided for binary compatibility with 1.6; it can be removed in 2.0. See ACCUMULO-3789.
      return decodeUnchecked(b, 0, b.length);
    }

    protected Integer decodeUnchecked(byte[] b, int offset, int len) {
      try {
        return Integer.parseInt(new String(b, offset, len, UTF_8));
      } catch (NumberFormatException nfe) {
        throw new ValueFormatException(nfe);
      }
    }
  }

  /**
   * A convenience method for setting the long encoding type.
   *
   * @param is
   *          IteratorSetting object to configure.
   * @param type
   *          LongCombiner.Type specifying the encoding type.
   */
  public static void setEncodingType(IteratorSetting is, IntCombiner.Type type) {
    is.addOption(TYPE, type.toString());
  }

  /**
   * A convenience method for setting the long encoding type.
   *
   * @param is
   *          IteratorSetting object to configure.
   * @param encoderClass
   *          {@code Class<? extends Encoder<Long>>} specifying the encoding type.
   */
  public static void setEncodingType(IteratorSetting is, Class<? extends Encoder<Integer>> encoderClass) {
    is.addOption(TYPE, CLASS_PREFIX + encoderClass.getName());
  }

  /**
   * A convenience method for setting the long encoding type.
   *
   * @param is
   *          IteratorSetting object to configure.
   * @param encoderClassName
   *          name of a class specifying the encoding type.
   */
  public static void setEncodingType(IteratorSetting is, String encoderClassName) {
    is.addOption(TYPE, CLASS_PREFIX + encoderClassName);
  }
}
