package co.cask.mmds.data;

import co.cask.mmds.stats.CategoricalHisto;
import co.cask.mmds.stats.Histograms;
import co.cask.mmds.stats.NumericHisto;

import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Statistics about columns in a DataSplit.
 */
public class ColumnStats {
  private final List<HistogramBin> histo;
  private final long totalCount;
  private final long nullCount;
  private final Double min;
  private final Double max;

  public ColumnStats(List<HistogramBin> histo, long totalCount, long nullCount) {
    this(histo, totalCount, nullCount, null, null);
  }

  public ColumnStats(NumericHisto histo) {
    this(Histograms.convert(histo), histo.getTotalCount(), histo.getNullCount(), histo.getMin(), histo.getMax());
  }

  public ColumnStats(CategoricalHisto histo) {
    this(Histograms.convert(histo), histo.getTotalCount(), histo.getNullCount());
  }

  public ColumnStats(List<HistogramBin> histo, long totalCount, long nullCount,
                     @Nullable Double min, @Nullable Double max) {
    this.histo = histo;
    this.totalCount = totalCount;
    this.nullCount = nullCount;
    this.min = min;
    this.max = max;
  }

  public List<HistogramBin> getHisto() {
    return histo;
  }

  public long getTotalCount() {
    return totalCount;
  }

  public long getNullCount() {
    return nullCount;
  }

  public Double getMin() {
    return min;
  }

  public Double getMax() {
    return max;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ColumnStats that = (ColumnStats) o;

    return Objects.equals(histo, that.histo) &&
      Objects.equals(totalCount, that.totalCount) &&
      Objects.equals(nullCount, that.nullCount) &&
      Objects.equals(min, that.min) &&
      Objects.equals(max, that.max);
  }

  @Override
  public int hashCode() {
    return Objects.hash(histo, min, max, totalCount, nullCount);
  }
}
