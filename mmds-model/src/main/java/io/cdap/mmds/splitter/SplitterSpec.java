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

package io.cdap.mmds.splitter;

import io.cdap.mmds.spec.ParamSpec;
import io.cdap.mmds.spec.ParamSpec;

import java.util.List;

/**
 * Splitter specification.
 */
public class SplitterSpec {
  private final String type;
  private final String label;
  private final List<ParamSpec> hyperparameters;

  public SplitterSpec(String type, String label, List<ParamSpec> hyperparameters) {
    this.type = type;
    this.label = label;
    this.hyperparameters = hyperparameters;
  }

  public String getType() {
    return type;
  }

  public String getLabel() {
    return label;
  }

  public List<ParamSpec> getHyperparameters() {
    return hyperparameters;
  }
}
