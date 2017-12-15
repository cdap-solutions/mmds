package co.cask.mmds.data;

import java.util.Objects;

/**
 * A bin in a histogram.
 */
public class HistogramBin {
  private final String bin;
  private final long count;

  public HistogramBin(String bin, long count) {
    this.bin = bin;
    this.count = count;
  }

  public String getBin() {
    return bin;
  }

  public long getCount() {
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

    HistogramBin that = (HistogramBin) o;

    return Objects.equals(bin, that.bin) && count == that.count;
  }

  @Override
  public int hashCode() {
    return Objects.hash(bin, count);
  }
}
