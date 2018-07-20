/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
