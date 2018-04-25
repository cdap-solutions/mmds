package co.cask.mmds.data;

import java.util.Objects;

/**
 * Value for test and training splits.
 *
 * @param <T> type of value
 */
public class SplitVal<T> {
  private final T train;
  private final T test;
  private final T total;

  public SplitVal(T train, T test, T total) {
    this.train = train;
    this.test = test;
    this.total = total;
  }

  public T getTrain() {
    return train;
  }

  public T getTest() {
    return test;
  }

  public T getTotal() {
    return total;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SplitVal<?> that = (SplitVal<?>) o;

    return Objects.equals(train, that.train) && Objects.equals(test, that.test) && Objects.equals(total, that.total);
  }

  @Override
  public int hashCode() {
    return Objects.hash(train, test, total);
  }
}
