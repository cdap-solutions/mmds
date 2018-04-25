package co.cask.mmds.data;

import java.util.Objects;

/**
 * Key for a model.
 */
public class ModelKey {
  private final String experiment;
  private final String model;

  public ModelKey(String experiment, String model) {
    this.experiment = experiment;
    this.model = model;
  }

  public String getExperiment() {
    return experiment;
  }

  public String getModel() {
    return model;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ModelKey that = (ModelKey) o;

    return Objects.equals(experiment, that.experiment) && Objects.equals(model, that.model);
  }

  @Override
  public int hashCode() {
    return Objects.hash(experiment, model);
  }

  @Override
  public String toString() {
    return "ModelKey{" +
      "experiment='" + experiment + '\'' +
      ", model='" + model + '\'' +
      '}';
  }
}
