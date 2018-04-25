package co.cask.mmds.spec;

/**
 * A range of numbers.
 */
public class Range {
  private final String min;
  private final String max;
  private final Boolean isMinInclusive;
  private final Boolean isMaxInclusive;

  public Range(String min, String max, Boolean isMinInclusive, Boolean isMaxInclusive) {
    this.min = min;
    this.max = max;
    this.isMinInclusive = isMinInclusive;
    this.isMaxInclusive = isMaxInclusive;
  }

  public Range(Integer min, boolean isMinInclusive) {
    this(min == null ? null : String.valueOf(min), null, isMinInclusive, null);
  }

  public Range(Integer min, Integer max, boolean isMinInclusive, boolean isMaxInclusive) {
    this(min == null ? null : String.valueOf(min), max == null ? null : String.valueOf(max),
         isMinInclusive, isMaxInclusive);
  }

  public Range(Long min, Long max, boolean isMinInclusive, boolean isMaxInclusive) {
    this(min == null ? null : String.valueOf(min), max == null ? null : String.valueOf(max),
         isMinInclusive, isMaxInclusive);
  }

  public Range(Double min, boolean isMinInclusive) {
    this(min == null ? null : String.valueOf(min), null, isMinInclusive, null);
  }

  public Range(Double min, Double max, boolean isMinInclusive, boolean isMaxInclusive) {
    this(min == null ? null : String.valueOf(min), max == null ? null : String.valueOf(max),
         isMinInclusive, isMaxInclusive);
  }
}
