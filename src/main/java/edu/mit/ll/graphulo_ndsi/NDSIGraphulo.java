package edu.mit.ll.graphulo_ndsi;

import com.google.common.base.Preconditions;
import edu.mit.ll.graphulo.Graphulo;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;

/**
 * Query on NDSI data
 */
public class NDSIGraphulo extends Graphulo {
  public NDSIGraphulo(Connector connector, PasswordToken password) {
    super(connector, password);
  }

  /** Pad long lat/lon to 5 digits. */
  public static final int PADSIZE_LATLON = 5;

  /**
   * Bin a subset of the input table, do aggregation in each bin of the subset, and put the results in a new table.
   *
   * @param Atable Input table name
   * @param Rtable Output table name
   * @param minX First row label in window
   * @param minY First column qualifier label in window
   * @param maxX Last row label in window
   * @param maxY Last column qualifier label in window
   * @param binsizeX Size of bins for rows
   * @param binsizeY Size of bins for column qualifiers
   * @return The number of entries processed in making the window subset.
   */
  public long windowSubset(String Atable, String Rtable,
                           long minX, long minY, long maxX, long maxY, double binsizeX, double binsizeY) {
    Atable = emptyToNull(Atable);
    Rtable = emptyToNull(Rtable);
    Preconditions.checkArgument(minX >= 0 && minY >= 0 && maxX > minX && maxY > minY
        && binsizeX > 0 && binsizeY > 0 && Atable != null && Rtable != null);

    String startX = StringUtils.leftPad(Long.toString(minX), PADSIZE_LATLON, '0');
    String startY = StringUtils.leftPad(Long.toString(minY), PADSIZE_LATLON, '0');
    String endX = StringUtils.leftPad(Long.toString(maxX), PADSIZE_LATLON, '0');
    String endY = StringUtils.leftPad(Long.toString(maxY), PADSIZE_LATLON, '0');
    String rowFilter = startX + ",:," + endX + ",";
    String colFilter = startY + ",:," + endY + ",";

    IteratorSetting itsetHistogram = Histogram2DTransformer.iteratorSetting(1, minX, minY, binsizeX, binsizeY);
    IteratorSetting itsetStats = DoubleStatsCombiner.iteratorSetting(PLUS_ITERATOR_BIGDECIMAL.getPriority(), null);

    // support transpose?
    // could reuse batchscanner if called many times
    return OneTable(Atable, Rtable, null, null, -1, null, null, itsetStats,
        rowFilter, colFilter, Collections.singletonList(itsetHistogram),
        null, Authorizations.EMPTY);
  }

}
