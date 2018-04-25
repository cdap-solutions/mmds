package co.cask.mmds.modeler.param;

import co.cask.mmds.spec.DoubleParam;
import co.cask.mmds.spec.IntParam;
import co.cask.mmds.spec.ParamSpec;
import co.cask.mmds.spec.Params;
import co.cask.mmds.spec.Range;

import java.util.List;
import java.util.Map;

/**
 * Modeler parameters for Gradient Boosted Tree algorithms.
 */
public class GBTParams extends TreeParams {
  protected final IntParam maxIterations;
  protected final DoubleParam subsamplingRate;
  protected final DoubleParam stepSize;

  public GBTParams(Map<String, String> modelParams) {
    super(modelParams);
    maxIterations = new IntParam("maxIterations", "Max Iterations", "maximum number of iterations",
                                 20, new Range(0, true), modelParams);
    subsamplingRate = new DoubleParam("subsamplingRate", "Sub-sampling Rate",
                                      "Fraction of the training data used for learning each decision tree.",
                                      1.0d, new Range(0d, 1d, false, true), modelParams);
    stepSize = new DoubleParam("stepSize", "Step Size",
                               "Step size (a.k.a. learning rate) for shrinking the contribution of each estimator.",
                               0.1d, new Range(0d, 1d, false, true), modelParams);
  }

  @Override
  public Map<String, String> toMap() {
    return Params.putParams(super.toMap(), maxIterations, subsamplingRate, stepSize);
  }

  @Override
  public List<ParamSpec> getSpec() {
    return Params.addParams(super.getSpec(), maxIterations, subsamplingRate, stepSize);
  }
}
