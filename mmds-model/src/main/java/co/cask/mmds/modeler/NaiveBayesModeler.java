package co.cask.mmds.modeler;

import co.cask.mmds.api.AlgorithmType;
import co.cask.mmds.api.Modeler;
import co.cask.mmds.modeler.param.ModelerParams;
import co.cask.mmds.modeler.param.NaiveBayesParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.linalg.Vector;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class NaiveBayesModeler implements Modeler<NaiveBayes, NaiveBayesModel> {

  @Override
  public AlgorithmType getType() {
    return AlgorithmType.CLASSIFICATION;
  }

  @Override
  public NaiveBayesParams getParams(Map<String, String> params) {
    return new NaiveBayesParams(params);
  }

  @Override
  public Predictor<Vector, NaiveBayes, NaiveBayesModel> createPredictor(Map<String, String> params) {
    NaiveBayesParams modelParams = getParams(params);
    NaiveBayes modeler = new NaiveBayes();
    modelParams.setParams(modeler);
    return modeler;
  }

  @Override
  public NaiveBayesModel loadPredictor(String path) {
    return NaiveBayesModel.load(path);
  }
}
