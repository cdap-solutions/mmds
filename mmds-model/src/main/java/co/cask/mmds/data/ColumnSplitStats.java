package co.cask.mmds.data;

import co.cask.mmds.NullableMath;
import co.cask.mmds.stats.CategoricalHisto;
import co.cask.mmds.stats.NumericBin;
import co.cask.mmds.stats.NumericHisto;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Statistics about columns in a DataSplit.
 */
public class ColumnSplitStats {
  private static final DecimalFormat NOTATION_FORMAT = new DecimalFormat("0.00E0");
  private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("###.####");
  private final String field;
  private final SplitVal<Long> numTotal;
  private final SplitVal<Long> numNull;
  // categorical
  private final SplitVal<Long> numEmpty;
  private final SplitVal<Long> unique;
  // numeric
  private final SplitVal<Long> numZero;
  private final SplitVal<Long> numPositive;
  private final SplitVal<Long> numNegative;
  private final SplitVal<Double> min;
  private final SplitVal<Double> max;
  private final SplitVal<Double> mean;
  private final SplitVal<Double> stddev;
  private final List<SplitHistogramBin> histo;
  private final double divergence;

  static {
    NOTATION_FORMAT.setRoundingMode(RoundingMode.HALF_UP);
  }

  public ColumnSplitStats(String field, SplitVal<Long> numTotal, SplitVal<Long> numNull, SplitVal<Long> numEmpty,
                          SplitVal<Long> unique, SplitVal<Long> numZero, SplitVal<Long> numPositive,
                          SplitVal<Long> numNegative, SplitVal<Double> min, SplitVal<Double> max, SplitVal<Double> mean,
                          SplitVal<Double> stddev, List<SplitHistogramBin> histo) {
    this.field = field;
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

    // Kullback-Leibler divergence, where each bin is treated as a value in the distribution
    double div = 0d;

    // add 1 to each bin to avoid divide by 0 issues
    double trainNonNull = numTotal.getTrain() - numNull.getTrain() + histo.size();
    double testNonNull = numTotal.getTest() - numNull.getTest() + histo.size();
    for (SplitHistogramBin bin : histo) {
      double trainProbability = (1 + bin.getCount().getTrain()) / trainNonNull;
      double testProbability = (1 + bin.getCount().getTest()) / testNonNull;
      div += testProbability * Math.log(testProbability / trainProbability);
    }
    // in case some floating point issue causes higher than 1 or lower than 0.
    this.divergence = Math.max(0.d, Math.min(1.0d, div));
  }

  public ColumnSplitStats(String field, NumericHisto train, NumericHisto test) {
    this(field,
         new SplitCountVal(train.getTotalCount(), test.getTotalCount()),
         new SplitCountVal(train.getNullCount(), test.getNullCount()),
         null, null,
         new SplitCountVal(train.getZeroCount(), test.getZeroCount()),
         new SplitCountVal(train.getPositiveCount(), test.getPositiveCount()),
         new SplitCountVal(train.getNegativeCount(), test.getNegativeCount()),
         new SplitVal<>(train.getMin(), test.getMin(), NullableMath.min(train.getMin(), test.getMin())),
         new SplitVal<>(train.getMax(), test.getMax(), NullableMath.max(train.getMax(), test.getMax())),
         new SplitVal<>(train.getMean(), test.getMean(),
                        NullableMath.mean(train.getMean(), train.getNonNullCount(),
                                          test.getMean(), test.getNonNullCount())),
         new SplitVal<>(train.getStddev(), test.getStddev(),
                        NullableMath.stddev(train.getM2(), train.getMean(), train.getNonNullCount(),
                                            test.getM2(), test.getMean(), test.getNonNullCount())),
         convert(train, test));
  }

  public ColumnSplitStats(String field, CategoricalHisto train, CategoricalHisto test) {
    this(field,
         new SplitCountVal(train.getTotalCount(), test.getTotalCount()),
         new SplitCountVal(train.getNullCount(), test.getNullCount()),
         new SplitCountVal(train.getEmptyCount(), test.getEmptyCount()),
         new SplitCountVal((long) train.getCounts().size(), (long) test.getCounts().size()),
         null, null, null, null, null, null, null,
         convert(train, test));
  }

  public List<SplitHistogramBin> getHisto() {
    return histo;
  }

  public String getField() {
    return field;
  }

  public SplitVal<Long> getNumTotal() {
    return numTotal;
  }

  public SplitVal<Long> getNumNull() {
    return numNull;
  }

  public SplitVal<Long> getNumEmpty() {
    return numEmpty;
  }

  public SplitVal<Long> getUnique() {
    return unique;
  }

  public SplitVal<Long> getNumZero() {
    return numZero;
  }

  public SplitVal<Long> getNumPositive() {
    return numPositive;
  }

  public SplitVal<Long> getNumNegative() {
    return numNegative;
  }

  public SplitVal<Double> getMin() {
    return min;
  }

  public SplitVal<Double> getMax() {
    return max;
  }

  public SplitVal<Double> getMean() {
    return mean;
  }

  public SplitVal<Double> getStddev() {
    return stddev;
  }

  public double getDivergence() {
    return divergence;
  }

  private static List<SplitHistogramBin> convert(NumericHisto train, NumericHisto test) {
    if (train.getBins().size() != test.getBins().size()) {
      throw new IllegalArgumentException("Cannot combine numeric histograms with different bins.");
    }

    List<SplitHistogramBin> bins = new ArrayList<>(train.getBins().size());

    Iterator<NumericBin> trainBins = train.getBins().iterator();
    Iterator<NumericBin> testBins = test.getBins().iterator();

    while (trainBins.hasNext()) {
      NumericBin bin1 = trainBins.next();
      NumericBin bin2 = testBins.next();

      if (bin1.getLo() != bin2.getLo() || bin1.getHi() != bin2.getHi() ||
        bin1.isHiInclusive() != bin2.isHiInclusive()) {
        throw new IllegalArgumentException(
          "Cannot combine numeric histograms with different bins. " +
            "Bin1 = " + format(bin1) + ", Bin2 = " + format(bin2));
      }

      String binStr = format(bin1);
      bins.add(new SplitHistogramBin(binStr, new SplitCountVal(bin1.getCount(), bin2.getCount())));
    }

    return bins;
  }

  public static List<SplitHistogramBin> convert(CategoricalHisto train, CategoricalHisto test) {
    List<SplitHistogramBin> bins = new ArrayList<>(train.getCounts().size());

    for (Map.Entry<String, Long> trainEntry : train.getCounts().entrySet()) {
      String category = trainEntry.getKey();
      Long trainCount = trainEntry.getValue();
      Long testCount = test.getCounts().get(category);
      bins.add(new SplitHistogramBin(category, new SplitCountVal(trainCount, testCount == null ? 0 : testCount)));
    }
    for (Map.Entry<String, Long> testEntry : test.getCounts().entrySet()) {
      String category = testEntry.getKey();
      Long testCount = testEntry.getValue();
      if (train.getCounts().containsKey(category)) {
        continue;
      }
      bins.add(new SplitHistogramBin(category, new SplitCountVal(0L, testCount)));
    }

    // sort in descending order
    bins.sort((h1, h2) -> {
      int cmp = Long.compare(h2.getCount().getTrain(), h1.getCount().getTrain());
      if (cmp != 0) {
        return cmp;
      }
      cmp = Long.compare(h2.getCount().getTest(), h1.getCount().getTest());
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
