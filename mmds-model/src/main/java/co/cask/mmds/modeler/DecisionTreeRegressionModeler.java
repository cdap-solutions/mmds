package co.cask.mmds.modeler;

import co.cask.mmds.api.AlgorithmType;
import co.cask.mmds.api.Modeler;
import co.cask.mmds.modeler.param.TreeParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class DecisionTreeRegressionModeler implements Modeler<DecisionTreeRegressor, DecisionTreeRegressionModel> {

  @Override
  public Algorithm getAlgorithm() {
    return new Algorithm(AlgorithmType.REGRESSION, "decision.tree.regression", "Decision Tree Regression");
  }

  @Override
  public TreeParams getParams(Map<String, String> params) {
    return new TreeParams(params);
  }

  @Override
  public Predictor<Vector, DecisionTreeRegressor, DecisionTreeRegressionModel>
  createPredictor(Map<String, String> params) {
    TreeParams modelParams = getParams(params);
    DecisionTreeRegressor modeler = new DecisionTreeRegressor();
    modelParams.setParams(modeler);
    return modeler;
  }

  @Override
  public DecisionTreeRegressionModel loadPredictor(String path) {
    return DecisionTreeRegressionModel.load(path);
  }
}
