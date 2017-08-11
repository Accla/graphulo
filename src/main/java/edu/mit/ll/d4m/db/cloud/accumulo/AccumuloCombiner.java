package edu.mit.ll.d4m.db.cloud.accumulo;
import org.apache.accumulo.core.iterators.Combiner;
import org.apache.accumulo.core.iterators.user.BigDecimalCombiner;
import org.apache.accumulo.core.iterators.user.MaxCombiner;
import org.apache.accumulo.core.iterators.user.MinCombiner;
import org.apache.accumulo.core.iterators.user.SummingCombiner;

import java.util.HashMap;
import java.util.Map;

public class AccumuloCombiner {

	public enum CombiningType {
		/*****************************************************************************************/
		/*************************** ADD NEW COMBINER CLASS TYPES HERE ***************************/
		/*****************************************************************************************/
		// The second number is the statically assigned priority of the combiner (lower is higher).
		//		Only matters if more than one combiner is set on a column.
		//		Note that 20 is the VersioningIterator combiner's priority -- don't go above that!
		SUM(SummingCombiner.class, 7),
		MAX(MaxCombiner.class, 8),
		MIN(MinCombiner.class, 9),
		SUM_DECIMAL(BigDecimalCombiner.BigDecimalSummingCombiner.class, 10),
		MAX_DECIMAL(BigDecimalCombiner.BigDecimalMaxCombiner.class, 11),
		MIN_DECIMAL(BigDecimalCombiner.BigDecimalMinCombiner.class, 12);

		private Class<? extends Combiner> cl;
		private int combinerPriority;

		static final String PREFIX = "CombiningType_";
		private static Map<String,CombiningType> nameMap;
		private static Map<Class<? extends Combiner>, CombiningType> classMap;

		static {
			nameMap = new HashMap<>();
			classMap = new HashMap<>();
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
		 * @return null if name not present, or the CombiningType if present
		 */
		public static CombiningType getByName(final String name) {
			return nameMap.get(name.toUpperCase());
		}
		public static CombiningType getByClass(final Class<? extends Combiner> name) {
			return classMap.get(name);
		}
		public static CombiningType getByClass(final String className) {
			try {
				return getByClass(Class.forName(className).asSubclass(Combiner.class));
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

}
