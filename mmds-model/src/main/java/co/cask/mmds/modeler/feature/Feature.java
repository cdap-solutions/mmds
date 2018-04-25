package co.cask.mmds.modeler.feature;

import java.util.Objects;

/**
 * Represents a field.
 */
public class Feature {
  private final String name;
  private final boolean isCategorical;

  public Feature(String name, boolean isCategorical) {
    this.name = name;
    this.isCategorical = isCategorical;
  }

  public String getName() {
    return name;
  }

  public boolean isCategorical() {
    return isCategorical;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Feature that = (Feature) o;

    return Objects.equals(name, that.name) && isCategorical == that.isCategorical;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, isCategorical);
  }
}
