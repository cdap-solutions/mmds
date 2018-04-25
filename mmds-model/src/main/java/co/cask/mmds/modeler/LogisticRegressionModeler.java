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
