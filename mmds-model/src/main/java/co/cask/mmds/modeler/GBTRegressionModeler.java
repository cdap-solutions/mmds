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
import co.cask.mmds.modeler.param.GBTRegressionParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.ml.regression.GBTRegressor;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class GBTRegressionModeler implements Modeler<GBTRegressor, GBTRegressionModel> {

  @Override
  public Algorithm getAlgorithm() {
    return new Algorithm(AlgorithmType.REGRESSION,
                         "gradient.boosted.tree.regression", "Gradient Boosted Tree Regression");
  }

  @Override
  public GBTRegressionParams getParams(Map<String, String> params) {
    return new GBTRegressionParams(params);
  }

  @Override
  public Predictor<Vector, GBTRegressor, GBTRegressionModel> createPredictor(Map<String, String> params) {
    GBTRegressionParams gbtParams = getParams(params);
    GBTRegressor modeler = new GBTRegressor();
    gbtParams.setParams(modeler);
    return modeler;
  }

  @Override
  public GBTRegressionModel loadPredictor(String path) {
    return GBTRegressionModel.load(path);
  }
}
