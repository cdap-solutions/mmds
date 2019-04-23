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

package io.cdap.mmds.api;

import io.cdap.mmds.modeler.Algorithm;
import io.cdap.mmds.spec.Parameters;
import org.apache.spark.ml.PredictionModel;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.linalg.Vector;

import java.util.Map;

/**
 * Creates a Spark {@link org.apache.spark.ml.Predictor} with the correct parameters set.
 */
public interface Modeler<P extends Predictor<Vector, P, M>, M extends PredictionModel<Vector, M>> {

  /**
   * @return algorithm of the model.
   */
  Algorithm getAlgorithm();

  /**
   * Get the modeler parameters specified in the string map along with any default parameters that were missing
   * from the map.
   *
   * @param params parameters as strings
   * @return updated parameters
   * @throws IllegalArgumentException if the map contains invalid parameters
   */
  Parameters getParams(Map<String, String> params);

  /**
   * Create the predictor used to train a model and make predictions.
   *
   * @params modeler parameters to configure the predictor with
   * @return predictor used to train a model and make predictions
   * @throws IllegalArgumentException if one of the modeler params is invalid
   */
  Predictor<Vector, P, M> createPredictor(Map<String, String> params);

  /**
   * Load a saved predictor.
   *
   * @param path directory containing the saved predictor
   * @return loaded predictor from a saved location
   */
  M loadPredictor(String path);
}
