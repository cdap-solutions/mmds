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

package co.cask.mmds.manager;

import co.cask.mmds.api.AlgorithmType;
import co.cask.mmds.modeler.Algorithm;
import co.cask.mmds.spec.ParamSpec;

import java.util.List;

/**
 * Describes an algorithm.
 */
public class AlgorithmSpec {
  private final AlgorithmType type;
  private final String algorithm;
  private final String label;
  private final List<ParamSpec> hyperparameters;

  public AlgorithmSpec(Algorithm algorithm, List<ParamSpec> hyperparameters) {
    this.type = algorithm.getType();
    this.algorithm = algorithm.getId();
    this.label = algorithm.getLabel();
    this.hyperparameters = hyperparameters;
  }
}
