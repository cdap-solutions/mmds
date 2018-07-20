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
import co.cask.mmds.modeler.param.NaiveBayesParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.linalg.Vector;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class NaiveBayesModeler implements Modeler<NaiveBayes, NaiveBayesModel> {

  @Override
  public Algorithm getAlgorithm() {
    return new Algorithm(AlgorithmType.CLASSIFICATION, "naive.bayes", "Naive Bayes");
  }

  @Override
  public NaiveBayesParams getParams(Map<String, String> params) {
    return new NaiveBayesParams(params);
  }

  @Override
  public Predictor<Vector, NaiveBayes, NaiveBayesModel> createPredictor(Map<String, String> params) {
    NaiveBayesParams modelParams = getParams(params);
    NaiveBayes modeler = new NaiveBayes();
    modelParams.setParams(modeler);
    return modeler;
  }

  @Override
  public NaiveBayesModel loadPredictor(String path) {
    return NaiveBayesModel.load(path);
  }
}
