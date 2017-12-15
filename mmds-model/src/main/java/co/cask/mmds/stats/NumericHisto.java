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

  public NumericHisto(double min, double max, int maxBins, Double val) {
    super(1L, val == null ? 1L : 0L);
    this.min = val;
    this.max = val;
    this.bins = new ArrayList<>();

    if (min == max) {
      NumericBin bin = new NumericBin(min, max, 0L, true);
      bin.incrementIfInBin(val);
      bins.add(bin);
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
      bin.incrementIfInBin(val);
      bins.add(bin);
      lo = hi;
      hi += step;
      numBins++;
    }
  }

  public NumericHisto(long totalCount, long nullCount, List<NumericBin> bins, Double min, Double max) {
    super(totalCount, nullCount);
    this.bins = new ArrayList<>(bins);
    this.min = min;
    this.max = max;
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

    return new NumericHisto(totalCount + other.totalCount, nullCount + other.nullCount, merged, newMin, newMax);
  }
}
