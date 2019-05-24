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
import io.cdap.mmds.modeler.param.TreeParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class DecisionTreeRegressionModeler implements Modeler<DecisionTreeRegressor, DecisionTreeRegressionModel> {

  @Override
  public Algorithm getAlgorithm() {
    return new Algorithm(AlgorithmType.REGRESSION, "decision.tree.regression", "Decision Tree Regression");
  }

  @Override
  public TreeParams getParams(Map<String, String> params) {
    return new TreeParams(params);
  }

  @Override
  public Predictor<Vector, DecisionTreeRegressor, DecisionTreeRegressionModel>
  createPredictor(Map<String, String> params) {
    TreeParams modelParams = getParams(params);
    DecisionTreeRegressor modeler = new DecisionTreeRegressor();
    modelParams.setParams(modeler);
    return modeler;
  }

  @Override
  public DecisionTreeRegressionModel loadPredictor(String path) {
    return DecisionTreeRegressionModel.load(path);
  }
}
