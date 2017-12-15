package co.cask.mmds.modeler;

import co.cask.mmds.api.AlgorithmType;
import co.cask.mmds.api.Modeler;
import co.cask.mmds.modeler.param.GeneralizedLinearRegressionParams;
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
  public AlgorithmType getType() {
    return AlgorithmType.REGRESSOR;
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
