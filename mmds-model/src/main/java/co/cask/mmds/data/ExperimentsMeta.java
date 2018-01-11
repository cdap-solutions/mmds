package co.cask.mmds.data;

import java.util.List;

/**
 * Holds information about total row count in experiment table along with list of experiments.
 */
public class ExperimentsMeta {
  private final long totalRowCount;
  private final List<Experiment> experiments;

  public ExperimentsMeta(long totalRowCount, List<Experiment> experiments) {
    this.totalRowCount = totalRowCount;
    this.experiments = experiments;
  }

  public long getTotalRowCount() {
    return totalRowCount;
  }

  public List<Experiment> getExperiments() {
    return experiments;
  }
}
