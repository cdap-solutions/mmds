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
