package co.cask.mmds.modeler;

import co.cask.mmds.api.AlgorithmType;
import co.cask.mmds.api.Modeler;
import co.cask.mmds.modeler.param.GBTClassifierParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.classification.GBTClassificationModel;
import org.apache.spark.ml.classification.GBTClassifier;
import org.apache.spark.ml.linalg.Vector;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class GBTClassifierModeler implements Modeler<GBTClassifier, GBTClassificationModel> {

  @Override
  public Algorithm getAlgorithm() {
    return new Algorithm(AlgorithmType.CLASSIFICATION,
                         "gradient.boosted.tree.classifier", "Gradient Boosted Tree Classifier");
  }

  @Override
  public GBTClassifierParams getParams(Map<String, String> params) {
    return new GBTClassifierParams(params);
  }

  @Override
  public Predictor<Vector, GBTClassifier, GBTClassificationModel> createPredictor(Map<String, String> params) {
    GBTClassifierParams gbtParams = getParams(params);
    GBTClassifier modeler = new GBTClassifier();
    gbtParams.setParams(modeler);
    return modeler;
  }

  @Override
  public GBTClassificationModel loadPredictor(String path) {
    return GBTClassificationModel.load(path);
  }
}
