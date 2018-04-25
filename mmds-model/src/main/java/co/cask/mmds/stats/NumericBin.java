package co.cask.mmds.stats;

import java.io.Serializable;

/**
 * A numeric histogram bin.
 */
public class NumericBin implements Serializable {
  private static final long serialVersionUID = -4458231951109307443L;
  private final double lo;
  private final double hi;
  private final boolean hiInclusive;
  private long count;

  public NumericBin(double lo, double hi, long count, boolean hiInclusive) {
    this.lo = lo;
    this.hi = hi;
    this.count = count;
    this.hiInclusive = hiInclusive;
  }

  public NumericBin merge(NumericBin other) {
    if (lo != other.lo || hi != other.hi || hiInclusive != other.hiInclusive) {
      throw new IllegalArgumentException("Cannot merge histograms with different bins.");
    }
    return new NumericBin(lo, hi, count + other.count, hiInclusive);
  }

  public boolean incrementIfInBin(Double val) {
    if (val == null) {
      return false;
    }
    if (val < lo) {
      return false;
    }
    if (val == hi && hiInclusive) {
      count++;
      return true;
    } else if (val < hi) {
      count++;
      return true;
    }
    return false;
  }

  public double getLo() {
    return lo;
  }

  public double getHi() {
    return hi;
  }

  public long getCount() {
    return count;
  }

  public boolean isHiInclusive() {
    return hiInclusive;
  }

  @Override
  public String toString() {
    return "NumericBin{" +
      "lo=" + lo +
      ", hi=" + hi +
      ", hiInclusive=" + hiInclusive +
      ", count=" + count +
      '}';
  }
}
