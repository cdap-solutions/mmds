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

  public SplitVal(T train, T test) {
    this.train = train;
    this.test = test;
  }

  public T getTrain() {
    return train;
  }

  public T getTest() {
    return test;
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

    return Objects.equals(train, that.train) && Objects.equals(test, that.test);
  }

  @Override
  public int hashCode() {
    return Objects.hash(train, test);
  }
}
