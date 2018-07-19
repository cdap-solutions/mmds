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
import co.cask.mmds.modeler.param.LogisticRegressionParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.linalg.Vector;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class LogisticRegressionModeler implements Modeler<LogisticRegression, LogisticRegressionModel> {

  @Override
  public Algorithm getAlgorithm() {
    return new Algorithm(AlgorithmType.CLASSIFICATION, "logistic.regression", "Logistic Regression");
  }

  @Override
  public LogisticRegressionParams getParams(Map<String, String> params) {
    return new LogisticRegressionParams(params);
  }

  @Override
  public Predictor<Vector, LogisticRegression, LogisticRegressionModel> createPredictor(Map<String, String> params) {
    LogisticRegressionParams modelParams = getParams(params);
    LogisticRegression modeler = new LogisticRegression().setFamily("multinomial");
    modelParams.setParams(modeler);
    return modeler;
  }

  @Override
  public LogisticRegressionModel loadPredictor(String path) {
    return LogisticRegressionModel.load(path);
  }
}
