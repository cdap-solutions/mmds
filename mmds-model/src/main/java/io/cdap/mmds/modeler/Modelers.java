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

package io.cdap.mmds.modeler;

import com.google.common.collect.ImmutableList;
import io.cdap.mmds.api.Modeler;
import io.cdap.mmds.api.Modeler;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Collection of built in Modelers.
 */
public class Modelers {
  private static final List<Modeler> MODELERS = ImmutableList.of(
    new DecisionTreeRegressionModeler(),
    new GeneralizedLinearRegressionModeler(),
    new GBTRegressionModeler(),
    new LinearRegressionModeler(),
    new RandomForestRegressionModeler(),
    new DecisionTreeClassifierModeler(),
    new GBTClassifierModeler(),
    new LogisticRegressionModeler(),
    new MultilayerPerceptronModeler(),
    new NaiveBayesModeler(),
    new RandomForestClassifierModeler());
  private static final Map<String, Modeler> MODELER_MAP = MODELERS.stream().collect(
    Collectors.toMap(modeler -> modeler.getAlgorithm().getId(), modeler -> modeler));

  public static Collection<String> getAlgorithms() {
    return MODELER_MAP.keySet();
  }

  public static Collection<Modeler> getModelers() {
    return MODELERS;
  }

  @Nullable
  public static Modeler getModeler(String algorithm) {
    return MODELER_MAP.get(algorithm);
  }
}
