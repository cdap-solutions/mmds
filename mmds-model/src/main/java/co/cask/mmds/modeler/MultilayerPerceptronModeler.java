package co.cask.mmds.modeler;

import co.cask.mmds.api.AlgorithmType;
import co.cask.mmds.api.Modeler;
import co.cask.mmds.modeler.param.MultilayerPerceptronParams;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.linalg.Vector;

import java.util.Map;

/**
 * Decision Tree for regression.
 */
public class MultilayerPerceptronModeler
  implements Modeler<MultilayerPerceptronClassifier, MultilayerPerceptronClassificationModel> {

  @Override
  public Algorithm getAlgorithm() {
    return new Algorithm(AlgorithmType.CLASSIFICATION,
                         "multilayer.perceptron.classifier", "Multi-Layer Perceptron Classifier");
  }

  @Override
  public MultilayerPerceptronParams getParams(Map<String, String> params) {
    return new MultilayerPerceptronParams(params);
  }

  @Override
  public Predictor<Vector, MultilayerPerceptronClassifier,
    MultilayerPerceptronClassificationModel> createPredictor(Map<String, String> params) {
    MultilayerPerceptronParams modelParams = getParams(params);
    MultilayerPerceptronClassifier modeler = new MultilayerPerceptronClassifier();
    modelParams.setParams(modeler);
    return modeler;
  }

  @Override
  public MultilayerPerceptronClassificationModel loadPredictor(String path) {
    return MultilayerPerceptronClassificationModel.load(path);
  }
}
