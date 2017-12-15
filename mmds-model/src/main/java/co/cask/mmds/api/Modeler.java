package co.cask.mmds.api;

import co.cask.mmds.modeler.param.ModelerParams;
import org.apache.spark.ml.PredictionModel;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.linalg.Vector;

import java.util.Map;

/**
 * Creates a Spark {@link org.apache.spark.ml.Predictor} with the correct parameters set.
 */
public interface Modeler<P extends Predictor<Vector, P, M>, M extends PredictionModel<Vector, M>> {

  /**
   * @return getType of model.
   */
  AlgorithmType getType();

  /**
   * Get the modeler parameters specified in the string map along with any default parameters that were missing
   * from the map.
   *
   * @param params parameters as strings
   * @return updated parameters
   * @throws IllegalArgumentException if the map contains invalid parameters
   */
  ModelerParams getParams(Map<String, String> params);

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
