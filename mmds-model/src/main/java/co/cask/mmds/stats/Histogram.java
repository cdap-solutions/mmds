package co.cask.mmds.stats;

import java.io.Serializable;

/**
 * Histogram.
 */
public abstract class Histogram<T extends Histogram> implements Serializable {
  private static final long serialVersionUID = -4477404207939331843L;
  protected long totalCount;
  protected long nullCount;

  protected Histogram(long totalCount, long nullCount) {
    this.totalCount = totalCount;
    this.nullCount = nullCount;
  }

  public long getTotalCount() {
    return totalCount;
  }

  public long getNullCount() {
    return nullCount;
  }

  public long getNonNullCount() {
    return totalCount - nullCount;
  }

  public abstract T merge(T other);
}
