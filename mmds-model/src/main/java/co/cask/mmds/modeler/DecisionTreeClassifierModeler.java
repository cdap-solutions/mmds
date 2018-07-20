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

package co.cask.mmds.modeler;

import co.cask.mmds.api.AlgorithmType;
import co.cask.mmds.api.Modeler;
import co.cask.mmds.modeler.param.DecisionTreeClassifierParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.linalg.Vector;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class DecisionTreeClassifierModeler implements Modeler<DecisionTreeClassifier, DecisionTreeClassificationModel> {

  @Override
  public Algorithm getAlgorithm() {
    return new Algorithm(AlgorithmType.CLASSIFICATION, "decision.tree.classifier", "Decision Tree Classifier");
  }

  @Override
  public DecisionTreeClassifierParams getParams(Map<String, String> params) {
    return new DecisionTreeClassifierParams(params);
  }

  @Override
  public Predictor<Vector, DecisionTreeClassifier,
    DecisionTreeClassificationModel> createPredictor(Map<String, String> params) {
    DecisionTreeClassifierParams modelParams = getParams(params);
    DecisionTreeClassifier modeler = new DecisionTreeClassifier();
    modelParams.setParams(modeler);
    return modeler;
  }

  @Override
  public DecisionTreeClassificationModel loadPredictor(String path) {
    return DecisionTreeClassificationModel.load(path);
  }
}
