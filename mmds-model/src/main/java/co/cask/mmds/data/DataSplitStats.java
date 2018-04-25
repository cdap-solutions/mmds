package co.cask.mmds.data;

import co.cask.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.Collections;
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
  private final SplitStatus status;
  private final List<ColumnSplitStats> stats;
  private final Set<String> models;

  public DataSplitStats(String id, String description, String type, Map<String, String> params, List<String> directives,
                        Schema schema, String trainingPath, String testPath, SplitStatus status,
                        List<ColumnSplitStats> stats, Set<String> models) {
    super(description, type, params, directives, schema);
    this.id = id;
    this.trainingPath = trainingPath;
    this.testPath = testPath;
    this.status = status;
    this.stats = stats;
    this.models = Collections.unmodifiableSet(models);
  }

  public String getId() {
    return id;
  }

  public SplitStatus getStatus() {
    return status;
  }

  @Nullable
  public String getTrainingPath() {
    return trainingPath;
  }

  @Nullable
  public String getTestPath() {
    return testPath;
  }

  public List<ColumnSplitStats> getStats() {
    return stats;
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
      Objects.equals(status, that.status) &&
      Objects.equals(stats, that.stats) &&
      Objects.equals(models, that.models);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, trainingPath, testPath, status, stats, models);
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
    private SplitStatus status;
    private List<ColumnSplitStats> stats;
    private Set<String> models;

    public Builder(String id) {
      this.id = id;
      models = new HashSet<>();
      stats = new ArrayList<>();
    }

    public Builder setTrainingPath(String trainingPath) {
      this.trainingPath = trainingPath;
      return this;
    }

    public Builder setTestPath(String testPath) {
      this.testPath = testPath;
      return this;
    }

    public Builder setStats(List<ColumnSplitStats> stats) {
      this.stats.clear();
      this.stats.addAll(stats);
      return this;
    }

    public Builder setModels(Set<String> models) {
      this.models.clear();
      this.models.addAll(models);
      return this;
    }

    public Builder setStatus(SplitStatus status) {
      this.status = status;
      return this;
    }

    public DataSplitStats build() {
      DataSplitStats splitStats = new DataSplitStats(id, description, type, params, directives, schema,
                                                     trainingPath, testPath, status, stats, models);
      splitStats.validate();
      return splitStats;
    }
  }

  @Override
  public String toString() {
    return "DataSplitStats{" +
      "id='" + id + '\'' +
      ", trainingPath='" + trainingPath + '\'' +
      ", testPath='" + testPath + '\'' +
      ", status='" + status + '\'' +
      ", stats=" + stats +
      ", models=" + models +
      "} " + super.toString();
  }
}
