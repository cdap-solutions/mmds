package co.cask.mmds.data;

import co.cask.cdap.api.data.schema.Schema;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Information and stats about a data split.
 */
public class DataSplitStats extends DataSplit {
  private final String id;
  private final String trainingPath;
  private final String testPath;
  private final Map<String, ColumnStats> trainingStats;
  private final Map<String, ColumnStats> testStats;
  private final Set<String> models;

  public DataSplitStats(String id, String description, String type, Map<String, String> params, List<String> directives,
                        Schema schema, String trainingPath, String testPath,
                        Map<String, ColumnStats> trainingStats, Map<String, ColumnStats> testStats,
                        Set<String> models) {
    super(description, type, params, directives, schema);
    this.id = id;
    this.trainingPath = trainingPath;
    this.testPath = testPath;
    this.trainingStats = Collections.unmodifiableMap(trainingStats);
    this.testStats = Collections.unmodifiableMap(testStats);
    this.models = Collections.unmodifiableSet(models);
  }

  public String getId() {
    return id;
  }

  @Nullable
  public String getTrainingPath() {
    return trainingPath;
  }

  @Nullable
  public String getTestPath() {
    return testPath;
  }

  public Map<String, ColumnStats> getTrainingStats() {
    return trainingStats;
  }

  public Map<String, ColumnStats> getTestStats() {
    return testStats;
  }

  public Set<String> getModels() {
    return models;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    DataSplitStats that = (DataSplitStats) o;

    return Objects.equals(id, that.id) &&
      Objects.equals(trainingPath, that.trainingPath) &&
      Objects.equals(testPath, that.testPath) &&
      Objects.equals(trainingStats, that.trainingStats) &&
      Objects.equals(testStats, that.testStats) &&
      Objects.equals(models, that.models);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, trainingPath, testPath, trainingStats, testStats, models);
  }

  /**
   * @return a builder to create DataSplitStats.
   */
  public static Builder builder(String id) {
    return new Builder(id);
  }

  /**
   * Builder to create DataSplitStats.
   */
  public static class Builder extends DataSplit.Builder<Builder> {
    private final String id;
    private String trainingPath;
    private String testPath;
    private Map<String, ColumnStats> trainingStats;
    private Map<String, ColumnStats> testStats;
    private Set<String> models;

    public Builder(String id) {
      this.id = id;
      models = new HashSet<>();
      trainingStats = new HashMap<>();
      testStats = new HashMap<>();
    }

    public Builder setTrainingPath(String trainingPath) {
      this.trainingPath = trainingPath;
      return this;
    }

    public Builder setTestPath(String testPath) {
      this.testPath = testPath;
      return this;
    }

    public Builder setTrainingStats(Map<String, ColumnStats> trainingStats) {
      this.trainingStats.clear();
      this.trainingStats.putAll(trainingStats);
      return this;
    }

    public Builder setTestStats(Map<String, ColumnStats> testStats) {
      this.testStats.clear();
      this.testStats.putAll(testStats);
      return this;
    }

    public Builder setModels(Set<String> models) {
      this.models.clear();
      this.models.addAll(models);
      return this;
    }

    public DataSplitStats build() {
      DataSplitStats stats = new DataSplitStats(id, description, type, params, directives, schema,
                                                trainingPath, testPath, trainingStats, testStats, models);
      stats.validate();
      return stats;
    }
  }

  @Override
  public String toString() {
    return "DataSplitStats{" +
      "id='" + id + '\'' +
      ", trainingPath='" + trainingPath + '\'' +
      ", testPath='" + testPath + '\'' +
      ", trainingStats=" + trainingStats +
      ", testStats=" + testStats +
      ", models=" + models +
      "} " + super.toString();
  }
}
