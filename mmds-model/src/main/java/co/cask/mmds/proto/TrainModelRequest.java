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

package co.cask.mmds.proto;

import co.cask.mmds.api.Modeler;
import co.cask.mmds.modeler.Modelers;
import com.google.common.base.Joiner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Request to train a model.
 */
public class TrainModelRequest {
  private final String algorithm;
  private final String predictionsDataset;
  private final Map<String, String> hyperparameters;

  public TrainModelRequest(String algorithm, String predictionsDataset, Map<String, String> hyperparameters) {
    this.algorithm = algorithm;
    this.predictionsDataset = predictionsDataset;
    this.hyperparameters = hyperparameters;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  @Nullable
  public String getPredictionsDataset() {
    return predictionsDataset;
  }

  public Map<String, String> getHyperparameters() {
    return Collections.unmodifiableMap(hyperparameters == null ? new HashMap<>() : hyperparameters);
  }

  /**
   * Validate the request this. This is used because this is normally created through deserializing user input,
   * which may be missing fields.
   *
   * @throws BadRequestException if the request is invalid
   */
  public void validate() {
    if (algorithm == null || algorithm.isEmpty()) {
      throw new BadRequestException("Must specify a name");
    }

    Modeler modeler = Modelers.getModeler(algorithm);
    if (modeler == null) {
      throw new BadRequestException(
        String.format("No modeler found for algorithm '%s'. Must be one of '%s'",
                      algorithm, Joiner.on(',').join(Modelers.getAlgorithms())));
    }
  }
}
