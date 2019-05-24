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
import io.cdap.mmds.modeler.param.GeneralizedLinearRegressionParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.GeneralizedLinearRegression;
import org.apache.spark.ml.regression.GeneralizedLinearRegressionModel;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class GeneralizedLinearRegressionModeler implements
        Modeler<GeneralizedLinearRegression, GeneralizedLinearRegressionModel> {

  @Override
  public Algorithm getAlgorithm() {
    return new Algorithm(AlgorithmType.REGRESSION, "generalized.linear.regression", "Generalized Linear Regression");
  }

  @Override
  public GeneralizedLinearRegressionParams getParams(Map<String, String> params) {
    return new GeneralizedLinearRegressionParams(params);
  }

  @Override
  public Predictor<Vector, GeneralizedLinearRegression,
    GeneralizedLinearRegressionModel> createPredictor(Map<String, String> params) {
    GeneralizedLinearRegressionParams modelParams = getParams(params);
    GeneralizedLinearRegression modeler = new GeneralizedLinearRegression();
    modelParams.setParams(modeler);
    return modeler;
  }

  @Override
  public GeneralizedLinearRegressionModel loadPredictor(String path) {
    return GeneralizedLinearRegressionModel.load(path);
  }
}
