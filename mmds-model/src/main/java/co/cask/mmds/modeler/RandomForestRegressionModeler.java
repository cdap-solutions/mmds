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
import co.cask.mmds.modeler.param.RandomForestRegressionParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class RandomForestRegressionModeler implements Modeler<RandomForestRegressor, RandomForestRegressionModel> {

  @Override
  public Algorithm getAlgorithm() {
    return new Algorithm(AlgorithmType.REGRESSION, "random.forest.regression", "Random Forest Regression");
  }

  @Override
  public RandomForestRegressionParams getParams(Map<String, String> params) {
    return new RandomForestRegressionParams(params);
  }

  @Override
  public Predictor<Vector, RandomForestRegressor,
    RandomForestRegressionModel> createPredictor(Map<String, String> params) {
    RandomForestRegressionParams forestParams = getParams(params);
    RandomForestRegressor modeler = new RandomForestRegressor();
    forestParams.setParams(modeler);
    return modeler;
  }

  @Override
  public RandomForestRegressionModel loadPredictor(String path) {
    return RandomForestRegressionModel.load(path);
  }
}
