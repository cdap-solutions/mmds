package co.cask.mmds.modeler;

import co.cask.mmds.api.AlgorithmType;
import co.cask.mmds.api.Modeler;
import co.cask.mmds.modeler.param.LinearRegressionParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class LinearRegressionModeler implements Modeler<LinearRegression, LinearRegressionModel> {

  @Override
  public AlgorithmType getType() {
    return AlgorithmType.REGRESSOR;
  }

  @Override
  public LinearRegressionParams getParams(Map<String, String> params) {
    return new LinearRegressionParams(params);
  }

  @Override
  public Predictor<Vector, LinearRegression, LinearRegressionModel> createPredictor(Map<String, String> params) {
    LinearRegressionParams modelParams = getParams(params);
    LinearRegression modeler = new LinearRegression();
    modelParams.setParams(modeler);
    return modeler;
  }

  @Override
  public LinearRegressionModel loadPredictor(String path) {
    return LinearRegressionModel.load(path);
  }
}
