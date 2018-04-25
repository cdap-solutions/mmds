package co.cask.mmds.data;

import co.cask.mmds.stats.CategoricalHisto;
import co.cask.mmds.stats.NumericBin;
import co.cask.mmds.stats.NumericHisto;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Statistics about a column.
 */
public class ColumnStats {
  private static final DecimalFormat NOTATION_FORMAT = new DecimalFormat("0.00E0");
  private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("###.####");
  private final Long numTotal;
  private final Long numNull;
  // categorical
  private final Long numEmpty;
  private final Integer unique;
  // numeric
  private final Long numZero;
  private final Long numPositive;
  private final Long numNegative;
  private final Double min;
  private final Double max;
  private final Double mean;
  private final Double stddev;
  private final List<HistogramBin> histo;

  static {
    NOTATION_FORMAT.setRoundingMode(RoundingMode.HALF_UP);
  }

  public ColumnStats(Long numTotal, Long numNull, Long numEmpty,
                     Integer unique, Long numZero, Long numPositive,
                     Long numNegative, Double min, Double max, Double mean,
                     Double stddev, List<HistogramBin> histo) {
    this.numTotal = numTotal;
    this.numNull = numNull;
    this.numEmpty = numEmpty;
    this.unique = unique;
    this.numZero = numZero;
    this.numPositive = numPositive;
    this.numNegative = numNegative;
    this.min = min;
    this.max = max;
    this.mean = mean;
    this.stddev = stddev;
    this.histo = histo;
  }

  public ColumnStats(NumericHisto histo) {
    this(histo.getTotalCount(), histo.getNullCount(), null, null,
         histo.getZeroCount(), histo.getPositiveCount(), histo.getNegativeCount(),
         histo.getMin(), histo.getMax(), histo.getMean(), histo.getStddev(), convert(histo));
  }

  public ColumnStats(CategoricalHisto histo) {
    this(histo.getTotalCount(), histo.getNullCount(), histo.getEmptyCount(), histo.getCounts().size(),
         null, null, null, null, null, null, null, convert(histo));
  }

  public List<HistogramBin> getHisto() {
    return histo;
  }

  private static List<HistogramBin> convert(NumericHisto histo) {
    List<HistogramBin> bins = new ArrayList<>(histo.getBins().size());

    for (NumericBin bin : histo.getBins()) {
      String binStr = format(bin);
      bins.add(new HistogramBin(binStr, bin.getCount()));
    }

    return bins;
  }

  public static List<HistogramBin> convert(CategoricalHisto histo) {
    List<HistogramBin> bins = new ArrayList<>(histo.getCounts().size());

    for (Map.Entry<String, Long> entry : histo.getCounts().entrySet()) {
      bins.add(new HistogramBin(entry.getKey(), entry.getValue()));
    }

    // sort in descending order
    bins.sort((h1, h2) -> {
      int cmp = Long.compare(h2.getCount(), h1.getCount());
      if (cmp != 0) {
        return cmp;
      }
      return h1.getBin().compareTo(h2.getBin());
    });
    return bins;
  }

  private static String format(NumericBin bin) {
    return String.format(bin.isHiInclusive() ? "[%s,%s]" : "[%s,%s)", format(bin.getLo()), format(bin.getHi()));
  }

  private static String format(double val) {
    double mag = Math.abs(val);
    return (mag > 1000d || (mag < 0.001d && mag > 0d) ? NOTATION_FORMAT : DECIMAL_FORMAT).format(val);
  }
}
