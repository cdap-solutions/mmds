package co.cask.mmds.modeler;

import co.cask.mmds.api.AlgorithmType;
import co.cask.mmds.api.Modeler;
import co.cask.mmds.modeler.param.DecisionTreeClassifierParams;
import co.cask.mmds.modeler.param.TreeParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.linalg.Vector;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class DecisionTreeClassifierModeler implements Modeler<DecisionTreeClassifier, DecisionTreeClassificationModel> {

  @Override
  public AlgorithmType getType() {
    return AlgorithmType.CLASSIFICATION;
  }

  @Override
  public DecisionTreeClassifierParams getParams(Map<String, String> params) {
    return new DecisionTreeClassifierParams(params);
  }

  @Override
  public Predictor<Vector, DecisionTreeClassifier,
    DecisionTreeClassificationModel> createPredictor(Map<String, String> params) {
    DecisionTreeClassifierParams modelParams = getParams(params);
    DecisionTreeClassifier modeler = new DecisionTreeClassifier();
    modelParams.setParams(modeler);
    return modeler;
  }

  @Override
  public DecisionTreeClassificationModel loadPredictor(String path) {
    return DecisionTreeClassificationModel.load(path);
  }
}
