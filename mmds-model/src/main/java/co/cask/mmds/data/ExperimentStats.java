/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.mmds.data;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Holds information about an Experiment
 */
public class ExperimentStats extends Experiment {
  private final Map<String, ColumnStats> evaluationMetrics;
  private final ColumnStats algorithms;
  private final ColumnStats statuses;

  public ExperimentStats(Experiment experiment, Map<String, ColumnStats> evaluationMetrics,
                         ColumnStats algorithms, ColumnStats statuses) {
    super(experiment.getName(), experiment.getDescription(), experiment.getSrcpath(), experiment.getOutcome(),
          experiment.getOutcomeType(), experiment.getDirectives());
    this.evaluationMetrics = Collections.unmodifiableMap(new HashMap<>(evaluationMetrics));
    this.algorithms = algorithms;
    this.statuses = statuses;
  }

  public Map<String, ColumnStats> getEvaluationMetrics() {
    return evaluationMetrics;
  }

  public ColumnStats getAlgorithms() {
    return algorithms;
  }

  public ColumnStats getStatuses() {
    return statuses;
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

    ExperimentStats that = (ExperimentStats) o;

    return Objects.equals(evaluationMetrics, that.evaluationMetrics) &&
      Objects.equals(algorithms, that.algorithms) &&
      Objects.equals(statuses, that.statuses);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), evaluationMetrics, algorithms, statuses);
  }

  @Override
  public String toString() {
    return "ExperimentStats{" +
      "evaluationMetrics=" + evaluationMetrics +
      ", algorithms=" + algorithms +
      ", statuses=" + statuses +
      "} " + super.toString();
  }
}
