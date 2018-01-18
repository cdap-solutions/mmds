package co.cask.mmds.modeler;

import co.cask.mmds.api.AlgorithmType;
import co.cask.mmds.api.Modeler;
import co.cask.mmds.modeler.param.RandomForestClassifierParams;
import co.cask.mmds.modeler.param.RandomForestParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.linalg.Vector;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class RandomForestClassifierModeler implements Modeler<RandomForestClassifier, RandomForestClassificationModel> {

  @Override
  public Algorithm getAlgorithm() {
    return new Algorithm(AlgorithmType.CLASSIFICATION, "random.forest.classifier", "Random Forest Classifier");
  }

  @Override
  public RandomForestParams getParams(Map<String, String> params) {
    return new RandomForestClassifierParams(params);
  }

  @Override
  public Predictor<Vector, RandomForestClassifier,
    RandomForestClassificationModel> createPredictor(Map<String, String> params) {
    RandomForestParams forestParams = getParams(params);
    RandomForestClassifier modeler = new RandomForestClassifier();
    forestParams.setParams(modeler);
    return modeler;
  }

  @Override
  public RandomForestClassificationModel loadPredictor(String path) {
    return RandomForestClassificationModel.load(path);
  }
}
