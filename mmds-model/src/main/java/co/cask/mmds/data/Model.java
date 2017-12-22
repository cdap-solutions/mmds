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
import javax.annotation.Nullable;

/**
 * Holds information about a Model
 */
public class Model {
  private final String name;
  private final String description;
  private final String algorithm;
  private final String split;
  private final String predictionsDataset;
  private final Map<String, String> hyperparameters;

  protected Model(String name, String description, String algorithm, String split,
                  @Nullable String predictionsDataset, Map<String, String> hyperparameters) {
    this.name = name;
    this.description = description;
    this.algorithm = algorithm;
    this.split = split;
    this.predictionsDataset = predictionsDataset;
    this.hyperparameters = hyperparameters;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description == null ? "" : description;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public String getSplit() {
    return split;
  }

  @Nullable
  public String getPredictionsDataset() {
    return predictionsDataset;
  }

  public Map<String, String> getHyperparameters() {
    return hyperparameters == null ? Collections.<String, String>emptyMap() : hyperparameters;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Model that = (Model) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(description, that.description) &&
      Objects.equals(algorithm, that.algorithm) &&
      Objects.equals(split, that.split) &&
      Objects.equals(predictionsDataset, that.predictionsDataset) &&
      Objects.equals(hyperparameters, that.hyperparameters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, algorithm, split, predictionsDataset, hyperparameters);
  }

  /**
   * @return builder to create a Model
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds a Model
   */
  public static class Builder<T extends Builder> {
    protected String name;
    protected String description;
    protected String algorithm;
    protected String split;
    protected String predictionsDataset;
    protected Map<String, String> hyperparameters;

    protected Builder() {
      hyperparameters = new HashMap<>();
    }

    public T setName(String name) {
      this.name = name;
      return (T) this;
    }

    public T setDescription(String description) {
      this.description = description;
      return (T) this;
    }

    public T setSplit(String split) {
      this.split = split;
      return (T) this;
    }

    public T setAlgorithm(String algorithm) {
      this.algorithm = algorithm;
      return (T) this;
    }

    public T setPredictionsDataset(String predictionsDataset) {
      this.predictionsDataset = predictionsDataset;
      return (T) this;
    }

    public T setHyperParameters(Map<String, String> hyperParameters) {
      this.hyperparameters.clear();
      this.hyperparameters.putAll(hyperParameters);
      return (T) this;
    }

    public Model build() {
      return new Model(name, description, algorithm, split, predictionsDataset, hyperparameters);
    }
  }

}
