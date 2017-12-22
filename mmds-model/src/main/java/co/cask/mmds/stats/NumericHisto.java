package co.cask.mmds.stats;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Histogram for a categorical column.
 */
public class NumericHisto extends Histogram<NumericHisto> implements Serializable {
  private static final long serialVersionUID = 6989232047290418068L;
  private final List<NumericBin> bins;
  private Double min;
  private Double max;
  private Double mean;
  private Double m2;
  private Long zeroCount;
  private Long negativeCount;
  private Long positiveCount;

  public NumericHisto(double min, double max, int maxBins, Double val) {
    super(0L, 0L);
    this.min = val;
    this.max = val;
    this.bins = new ArrayList<>();
    this.zeroCount = 0L;
    this.negativeCount = 0L;
    this.positiveCount = 0L;
    this.mean = null;
    this.m2 = null;

    if (min == max) {
      NumericBin bin = new NumericBin(min, max, 0L, true);
      bins.add(bin);
      update(val);
      return;
    }

    double roundedMin = round(min, 2, Math::floor);
    double step = round((max - roundedMin) / maxBins, 1, Math::ceil);
    double lo = roundedMin;
    double hi = roundedMin + step;
    int numBins = 1;
    while (lo < max) {
      // double arithmetic is not exact, make sure final bin always includes the max.
      hi = numBins == maxBins ? Math.max(hi, max) : hi;
      NumericBin bin = new NumericBin(lo, hi, 0L, hi >= max);
      bins.add(bin);
      lo = hi;
      hi += step;
      numBins++;
    }

    update(val);
  }

  private NumericHisto(long totalCount, long nullCount, List<NumericBin> bins, Double min, Double max, Double mean,
                       Double m2, Long zeroCount, Long negativeCount, Long positiveCount) {
    super(totalCount, nullCount);
    this.bins = bins;
    this.min = min;
    this.max = max;
    this.mean = mean;
    this.m2 = m2;
    this.zeroCount = zeroCount;
    this.negativeCount = negativeCount;
    this.positiveCount = positiveCount;
  }

  /**
   * Rounds the given number to the n'th most significant digit.
   */
  private double round(double x, int n, Function<Double, Double> rounder) {
    if (x == 0) {
      return x;
    }
    int multiplier = 1;
    if (x < 0) {
      multiplier = -1;
      x = 0 - x;
    }
    double exponent = Math.floor(Math.log10(x));

    double power = Math.pow(10d, exponent - n + 1);
    // make it a 3 digit number
    double scaled = x / power;
    double rounded = rounder.apply(scaled);

    return multiplier * rounded * power;
  }

  public List<NumericBin> getBins() {
    return bins;
  }

  public Double getMin() {
    return min;
  }

  public Double getMax() {
    return max;
  }

  public Double getMean() {
    return mean;
  }

  public Double getStddev() {
    return m2 == null ? null : Math.sqrt(m2 / (totalCount - nullCount));
  }

  public Long getZeroCount() {
    return zeroCount;
  }

  public Long getNegativeCount() {
    return negativeCount;
  }

  public Long getPositiveCount() {
    return positiveCount;
  }

  public void update(Double val) {
    totalCount++;
    if (val == null) {
      nullCount++;
      return;
    }

    if (min == null) {
      min = val;
    } else {
      min = Math.min(min, val);
    }
    if (max == null) {
      max = val;
    } else {
      max = Math.max(max, val);
    }

    if (val == 0d) {
      zeroCount++;
    } else if (val > 0d) {
      positiveCount++;
    } else {
      negativeCount++;
    }

    if (mean == null) {
      mean = val;
      m2 = 0d;
    } else {
      // see https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
      long numNonNull = totalCount - nullCount;
      double delta = val - mean;
      mean += delta / numNonNull;
      double delta2 = val - mean;
      m2 += delta * delta2;
    }

    for (NumericBin bin : bins) {
      if (bin.incrementIfInBin(val)) {
        return;
      }
    }
  }

  @Override
  public NumericHisto merge(NumericHisto other) {
    List<NumericBin> merged = new ArrayList<>(bins.size());
    Iterator<NumericBin> h1Iter = bins.iterator();
    Iterator<NumericBin> h2Iter = other.bins.iterator();
    while (h1Iter.hasNext()) {
      merged.add(h1Iter.next().merge(h2Iter.next()));
    }

    Double newMin;
    Double newMax;
    if (min != null && other.min != null) {
      newMin = Math.min(min, other.min);
    } else if (min == null) {
      newMin = other.min;
    } else {
      newMin = min;
    }
    if (max != null && other.max != null) {
      newMax = Math.max(max, other.max);
    } else if (max == null) {
      newMax = other.max;
    } else {
      newMax = max;
    }

    long newTotalCount = totalCount + other.totalCount;
    long newNullCount = nullCount + other.nullCount;
    long newZeroCount = zeroCount + other.zeroCount;
    long newPositiveCount = positiveCount + other.positiveCount;
    long newNegativeCount = negativeCount + other.negativeCount;

    long nonNullCount = totalCount - nullCount;
    long otherNonNullCount = other.totalCount - other.nullCount;
    long totalNonNullCount = nonNullCount + otherNonNullCount;
    Double newMean;
    if (mean == null && other.mean == null) {
      newMean = null;
    } else if (mean == null) {
      newMean = other.mean;
    } else if (other.mean == null) {
      newMean = mean;
    } else {
      newMean = mean * nonNullCount + other.mean * otherNonNullCount;
      newMean /= totalNonNullCount;
    }

    Double newM2;
    if (m2 == null && other.m2 == null) {
      newM2 = null;
    } else if (m2 == null) {
      newM2 = other.m2;
    } else if (other.m2 == null) {
      newM2 = m2;
    } else {
      newM2 = m2 + other.m2 + nonNullCount * square(mean - newMean) + otherNonNullCount * square(other.mean - newMean);
    }

    return new NumericHisto(newTotalCount, newNullCount, merged, newMin, newMax, newMean, newM2,
                            newZeroCount, newNegativeCount, newPositiveCount);
  }

  private double square(double x) {
    return x * x;
  }
}
