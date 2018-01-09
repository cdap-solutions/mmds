package co.cask.mmds;

import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

/**
 * Math for nullable values.
 */
public final class NullableMath {

  private NullableMath() {
    // private constructor for utility class
  }

  /**
   * Return the min of the specified values
   */
  public static Double min(Double x, Double y) {
    return apply(x, y, Math::min);
  }

  /**
   * Return the max of the specified values
   */
  public static Double max(Double x, Double y) {
    return apply(x, y, Math::max);
  }

  /**
   * Return the mean of the specified means
   */
  public static Double mean(Double mean1, long count1, Double mean2, long count2) {
    return apply(mean1, mean2, (m1, m2) -> (m1 * count1 + m2 * count2) / (count1 + count2));
  }

  /**
   * Return the m2 (variance * count) of the specified m2 values
   */
  public static Double m2(Double firstM2, Double mean1, long count1, Double secondM2, Double mean2, long count2) {
    Double newMean = mean(mean1, count1, mean2, count2);
    return apply(firstM2, secondM2, (m21, m22) ->
      m21 + m22 + count1 * square(mean1 - newMean) + count2 * square(mean2 - newMean));
  }

  /**
   * Return the stddev from the specified m2 values
   */
  public static Double stddev(Double firstM2, Double mean1, long count1, Double secondM2, Double mean2, long count2) {
    return Math.sqrt(m2(firstM2, mean1, count1, secondM2, mean2, count2) / (count1 + count2));
  }

  public static Double square(Double x) {
    if (x == null) {
      return x;
    }
    return x * x;
  }

  /**
   * If either x or y is null, returns the other value. If both are non-null, applies the BiFunction to x and y.
   */
  public static <T> T apply(T x, T y, BinaryOperator<T> func) {
    if (x != null && y != null) {
      return func.apply(x, y);
    } else if (x != null) {
      return x;
    } else {
      return y;
    }
  }
}
