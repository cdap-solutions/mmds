package co.cask.mmds.modeler.feature;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Set;

/**
 * Loads a feature generation model and uses it to generate features. Used in predictors.
 */
public class FeatureGeneratorPredictor extends FeatureGenerator {
  private final String featureGenPath;

  public FeatureGeneratorPredictor(List<String> features, Set<String> categoricalFeatures, String featureGenPath) {
    super(features, categoricalFeatures);
    this.featureGenPath = featureGenPath;
  }

  @Override
  protected PipelineModel getFeatureGenModel(Dataset<Row> cleanData) {
    return PipelineModel.load(featureGenPath);
  }
}
