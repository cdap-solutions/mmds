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

import io.cdap.mmds.api.AlgorithmType;
import io.cdap.mmds.api.Modeler;
import io.cdap.mmds.modeler.param.GBTClassifierParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.linalg.Vector;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class GBTClassifierModeler implements Modeler<GBTClassifier, GBTClassificationModel> {

  @Override
  public Algorithm getAlgorithm() {
    return new Algorithm(AlgorithmType.CLASSIFICATION,
                         "gradient.boosted.tree.classifier", "Gradient Boosted Tree Classifier");
  }

  @Override
  public GBTClassifierParams getParams(Map<String, String> params) {
    return new GBTClassifierParams(params);
  }

  @Override
  public Predictor<Vector, GBTClassifier, GBTClassificationModel> createPredictor(Map<String, String> params) {
    GBTClassifierParams gbtParams = getParams(params);
    GBTClassifier modeler = new GBTClassifier();
    gbtParams.setParams(modeler);
    return modeler;
  }

  @Override
  public GBTClassificationModel loadPredictor(String path) {
    return GBTClassificationModel.load(path);
  }
}
