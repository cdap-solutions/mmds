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
import io.cdap.mmds.modeler.param.MultilayerPerceptronParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.linalg.Vector;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class MultilayerPerceptronModeler
  implements Modeler<MultilayerPerceptronClassifier, MultilayerPerceptronClassificationModel> {

  @Override
  public Algorithm getAlgorithm() {
    return new Algorithm(AlgorithmType.CLASSIFICATION,
                         "multilayer.perceptron.classifier", "Multi-Layer Perceptron Classifier");
  }

  @Override
  public MultilayerPerceptronParams getParams(Map<String, String> params) {
    return new MultilayerPerceptronParams(params);
  }

  @Override
  public Predictor<Vector, MultilayerPerceptronClassifier,
    MultilayerPerceptronClassificationModel> createPredictor(Map<String, String> params) {
    MultilayerPerceptronParams modelParams = getParams(params);
    MultilayerPerceptronClassifier modeler = new MultilayerPerceptronClassifier();
    modelParams.setParams(modeler);
    return modeler;
  }

  @Override
  public MultilayerPerceptronClassificationModel loadPredictor(String path) {
    return MultilayerPerceptronClassificationModel.load(path);
  }
}
