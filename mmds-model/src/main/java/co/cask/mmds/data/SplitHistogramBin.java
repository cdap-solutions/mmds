package co.cask.mmds.data;

import java.util.Objects;

/**
 * A bin in a histogram.
 */
public class SplitHistogramBin {
  private final String bin;
  private final SplitVal<Long> count;

  public SplitHistogramBin(String bin, SplitVal<Long> count) {
    this.bin = bin;
    this.count = count;
  }

  public String getBin() {
    return bin;
  }

  public SplitVal<Long> getCount() {
    return count;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SplitHistogramBin that = (SplitHistogramBin) o;

    return Objects.equals(bin, that.bin) && Objects.equals(count, that.count);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bin, count);
  }
}
