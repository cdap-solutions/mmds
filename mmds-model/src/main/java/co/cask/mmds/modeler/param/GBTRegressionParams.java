package co.cask.mmds.modeler.param;

import co.cask.mmds.modeler.param.spec.ParamSpec;
import co.cask.mmds.modeler.param.spec.Params;
import co.cask.mmds.modeler.param.spec.StringParam;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.ml.regression.GBTRegressor;

import java.util.List;
import java.util.Map;

/**
 * Modeler parameters for Gradient Boosted Tree algorithms.
 */
public class GBTRegressionParams extends GBTParams {
  private final StringParam lossType;

  public GBTRegressionParams(Map<String, String> modelParams) {
    super(modelParams);
    // squared or absolute
    this.lossType = new StringParam("lossType",
                                    "Loss function which GBT tries to minimize. " +
                                      "Supports 'squared' (L2) and 'absolute' (L1)",
                                    "squared", ImmutableSet.of("squared", "absolute"), modelParams);
  }

  public void setParams(GBTRegressor gbt) {
    super.setParams(gbt);
    gbt.setMaxIter(maxIterations.getVal());
    gbt.setSubsamplingRate(subsamplingRate.getVal());
    gbt.setStepSize(stepSize.getVal());
    gbt.setLossType(lossType.getVal());
  }

  @Override
  public Map<String, String> toMap() {
    return Params.putParams(super.toMap(), lossType);
  }

  @Override
  public List<ParamSpec> getSpec() {
    return Params.addParams(super.getSpec(), lossType);
  }
}
