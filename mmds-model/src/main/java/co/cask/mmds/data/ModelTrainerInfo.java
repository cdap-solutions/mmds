package co.cask.mmds.data;

import java.util.Objects;

/**
 * Information required to train a model.
 */
public class ModelTrainerInfo {
  private final Experiment experiment;
  private final DataSplitStats dataSplitStats;
  private final String modelId;
  private final Model model;

  public ModelTrainerInfo(Experiment experiment, DataSplitStats dataSplitStats, String modelId, Model model) {
    this.experiment = experiment;
    this.dataSplitStats = dataSplitStats;
    this.modelId = modelId;
    this.model = model;
  }

  public Experiment getExperiment() {
    return experiment;
  }

  public DataSplitStats getDataSplitStats() {
    return dataSplitStats;
  }

  public String getModelId() {
    return modelId;
  }

  public Model getModel() {
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

    ModelTrainerInfo that = (ModelTrainerInfo) o;

    return Objects.equals(experiment, that.experiment) &&
      Objects.equals(dataSplitStats, that.dataSplitStats) &&
      Objects.equals(modelId, that.modelId) &&
      Objects.equals(model, that.model);
  }

  @Override
  public int hashCode() {
    return Objects.hash(experiment, dataSplitStats, modelId, model);
  }
}
