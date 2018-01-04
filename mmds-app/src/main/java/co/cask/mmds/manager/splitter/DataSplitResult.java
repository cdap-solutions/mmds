package co.cask.mmds.manager.splitter;

import co.cask.mmds.data.ColumnSplitStats;

import java.util.List;

/**
 * Results of actually splitting data.
 */
public class DataSplitResult {
  private final String trainingPath;
  private final String testPath;
  private final List<ColumnSplitStats> stats;

  public DataSplitResult(String trainingPath, String testPath, List<ColumnSplitStats> stats) {
    this.trainingPath = trainingPath;
    this.testPath = testPath;
    this.stats = stats;
  }

  public String getTrainingPath() {
    return trainingPath;
  }

  public String getTestPath() {
    return testPath;
  }

  public List<ColumnSplitStats> getStats() {
    return stats;
  }
}
