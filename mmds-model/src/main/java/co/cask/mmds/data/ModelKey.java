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
