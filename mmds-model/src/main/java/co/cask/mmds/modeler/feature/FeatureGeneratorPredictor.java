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

package co.cask.mmds.modeler.feature;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Set;

/**
 * Loads a feature generation model and uses it to generate features. Used in predictors.
 */
public class FeatureGeneratorPredictor extends FeatureGenerator {
  private final String featureGenPath;

  public FeatureGeneratorPredictor(List<String> features, Set<String> categoricalFeatures, String featureGenPath) {
    super(features, categoricalFeatures);
    this.featureGenPath = featureGenPath;
  }

  @Override
  protected PipelineModel getFeatureGenModel(Dataset<Row> cleanData) {
    return PipelineModel.load(featureGenPath);
  }
}
