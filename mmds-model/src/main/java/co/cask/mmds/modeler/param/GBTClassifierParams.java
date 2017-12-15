package co.cask.mmds.modeler.param;

import org.apache.spark.ml.classification.GBTClassifier;

import java.util.Map;

/**
 * Modeler parameters for Gradient Boosted Tree algorithms.
 */
public class GBTClassifierParams extends GBTParams {

  public GBTClassifierParams(Map<String, String> modelParams) {
    super(modelParams);
  }

  public void setParams(GBTClassifier gbt) {
    super.setParams(gbt);
    gbt.setMaxIter(maxIterations.getVal());
    gbt.setSubsamplingRate(subsamplingRate.getVal());
    gbt.setStepSize(stepSize.getVal());
  }

}
