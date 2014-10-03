package edu.mit.ll.d4m.db.cloud.accumulo;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.TypedValueCombiner;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.accumulo.core.iterators.user.MaxCombiner;
import org.apache.accumulo.core.iterators.user.MinCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.io.Text;

//import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations.BigDecimalEncoder;
//import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations.CombiningType;
//import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations.CombiningType.BigDecimalMaxCombiner;
//import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations.CombiningType.BigDecimalMinCombiner;
//import edu.mit.ll.d4m.db.cloud.D4mDbTableOperations.CombiningType.BigDecimalSummingCombiner;
//import edu.mit.ll.d4m.db.cloud.util.D4mQueryUtil;

public class AccumuloCombiner {

	public static enum CombiningType { 
		/*****************************************************************************************/
		/*************************** ADD NEW COMBINER CLASS TYPES HERE ***************************/
		/*****************************************************************************************/
		// The second number is the statically assigned priority of the combiner (lower is higher).
		//		Only matters if more than one combiner is set on a column.
		//		Note that 20 is the VersioningIterator combiner's priority -- don't go above that!
		SUM(SummingCombiner.class, 7),
		MAX(MaxCombiner.class, 8),
		MIN(MinCombiner.class, 9),
		SUM_DECIMAL(BigDecimalSummingCombiner.class, 10), 
		MAX_DECIMAL(BigDecimalMaxCombiner.class, 11),
		MIN_DECIMAL(BigDecimalMinCombiner.class, 12);

		public Class<? extends Combiner> cl;
		private int combinerPriority;

		static final String PREFIX = "CombiningType_";
		private static Map<String,CombiningType> nameMap;
		private static Map<Class<? extends Combiner>, CombiningType> classMap;

		static {
			nameMap = new HashMap<String,CombiningType>();
			classMap = new HashMap<Class<? extends Combiner>, CombiningType>();
			for (CombiningType ct : CombiningType.values()) {
				nameMap.put(ct.name().toUpperCase(), ct);
				classMap.put(ct.cl, ct);
			}
		}

		CombiningType(Class<? extends Combiner> cl, int combinerPriority) {
			this.cl = cl;
			this.combinerPriority = combinerPriority;
		}

		/**
		 * Lookup a CombiningType by name (case insensitive)
		 * @param name
		 * @return null if name not present, or the CombiningType if present
		 */
		public static CombiningType getByName(final String name) {
			return nameMap.get(name.toUpperCase());
		}
		public static CombiningType getByClass(final Class<?> name) {
			return classMap.get(name);
		}
		public static CombiningType getByClass(final String className) {
			try {
				return getByClass(Class.forName(className));
			} catch(ClassNotFoundException e) {
				return null;
			}
		}

		public Class<? extends Combiner> getCl() {
			return cl;
		}

		public int getCombinerPriority() {
			return combinerPriority;
		}
		public String getIteratorName() {
			return PREFIX+this.name();
		}
	} // end CombiningType enum

	/* *************************************************************************************************
	 * Begin Combiner Class Definitions
	 */
	public static class BigDecimalSummingCombiner extends TypedValueCombiner<BigDecimal>
	{
		private final static BigDecimalEncoder BDE = new BigDecimalEncoder();
		@Override
		public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
			super.init(source, options, env);
			setEncoder(BDE);
		}

		@Override
		public BigDecimal typedReduce(Key key, Iterator<BigDecimal> iter) {
			if (!iter.hasNext())
				return null;
			BigDecimal sum = iter.next();
			while (iter.hasNext()) {
				sum = sum.add(iter.next());
			}
			return sum;
		}
	}
	public static class BigDecimalMaxCombiner extends TypedValueCombiner<BigDecimal>
	{
		private final static BigDecimalEncoder BDE = new BigDecimalEncoder();
		@Override
		public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
			super.init(source, options, env);
			setEncoder(BDE);
		}

		@Override
		public BigDecimal typedReduce(Key key, Iterator<BigDecimal> iter) {
			if (!iter.hasNext())
				return null;
			BigDecimal max = iter.next();
			while (iter.hasNext()) {
				max = max.max(iter.next());
			}
			return max;
		}
	}
	public static class BigDecimalMinCombiner extends TypedValueCombiner<BigDecimal>
	{
		private final static BigDecimalEncoder BDE = new BigDecimalEncoder();
		@Override
		public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
			super.init(source, options, env);
			setEncoder(BDE);
		}

		@Override
		public BigDecimal typedReduce(Key key, Iterator<BigDecimal> iter) {
			if (!iter.hasNext())
				return null;
			BigDecimal min = iter.next();
			while (iter.hasNext()) {
				min = min.min(iter.next());
			}
			return min;
		}
	}


	/**
	 * Provides the ability to encode scientific notation.
	 * @author dy23798
	 *
	 */
	public static class BigDecimalEncoder implements org.apache.accumulo.core.iterators.TypedValueCombiner.Encoder<BigDecimal> {
		@Override
		public byte[] encode(BigDecimal v) {
			return v.toString().getBytes();
		}

		@Override
		public BigDecimal decode(byte[] b) throws ValueFormatException {
			try {
				return new BigDecimal(new String(b));
			} catch (NumberFormatException nfe) {
				throw new ValueFormatException(nfe);
			}
		}
	}




}
