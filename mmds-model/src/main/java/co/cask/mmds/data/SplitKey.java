package co.cask.mmds.data;

import java.util.Objects;

/**
 * Key for a split.
 */
public class SplitKey {
  private final String experiment;
  private final String split;

  public SplitKey(String experiment, String split) {
    this.experiment = experiment;
    this.split = split;
  }

  public String getExperiment() {
    return experiment;
  }

  public String getSplit() {
    return split;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SplitKey that = (SplitKey) o;

    return Objects.equals(experiment, that.experiment) && Objects.equals(split, that.split);
  }

  @Override
  public int hashCode() {
    return Objects.hash(experiment, split);
  }

  @Override
  public String toString() {
    return "ModelKey{" +
      "experiment='" + experiment + '\'' +
      ", split='" + split + '\'' +
      '}';
  }
}
