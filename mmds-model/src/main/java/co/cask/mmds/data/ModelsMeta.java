package co.cask.mmds.data;

import java.util.List;

/**
 * Holds information about total row count of models table along with list of models.
 */
public class ModelsMeta {
  private final long totalRowCount;
  private final List<ModelMeta> models;

  public ModelsMeta(long totalRowCount, List<ModelMeta> models) {
    this.totalRowCount = totalRowCount;
    this.models = models;
  }

  public long getTotalRowCount() {
    return totalRowCount;
  }

  public List<ModelMeta> getModels() {
    return models;
  }
}
