package co.cask.mmds.modeler.param;

import co.cask.mmds.modeler.param.spec.BoolParam;
import co.cask.mmds.modeler.param.spec.DoubleParam;
import co.cask.mmds.modeler.param.spec.IntParam;
import co.cask.mmds.modeler.param.spec.ParamSpec;
import co.cask.mmds.modeler.param.spec.Params;
import co.cask.mmds.modeler.param.spec.Range;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Common modeler parameters for regression based algorithms.
 */
public abstract class RegressionParams implements ModelerParams {
  protected final IntParam maxIterations;
  protected final BoolParam standardization;
  protected final DoubleParam regularizationParam;
  protected final DoubleParam elasticNetParam;
  protected final DoubleParam tolerance;

  protected RegressionParams(Map<String, String> modelParams) {
    maxIterations = new IntParam("maxIterations", "max number of iterations", 100, new Range(1, true), modelParams);
    standardization = new BoolParam("standardization",
                                    "Whether to standardize the training features before fitting the model.",
                                    true, modelParams);
    regularizationParam = new DoubleParam("regularizationParam", "regularization parameter", 0.0d,
                                          new Range(0d, true), modelParams);
    elasticNetParam = new DoubleParam("elasticNetParam",
                                      "The ElasticNet mixing parameter. " +
                                        "For alpha = 0, the penalty is an L2 penalty. " +
                                        "For alpha = 1, it is an L1 penalty. " +
                                        "For alpha in (0,1), the penalty is a combination of L1 and L2.",
                                      0.0d, new Range(0d, 1d, true, true), modelParams);
    tolerance = new DoubleParam("tolerance",
                                "Convergence tolerance of iterations. " +
                                  "Smaller values will lead to higher accuracy with the cost of more iterations.",
                                0.000001d, new Range(0d, true), modelParams);
  }

  @Override
  public List<ParamSpec> getSpec() {
    List<ParamSpec> specs = new ArrayList<>();
    return Params.addParams(specs, maxIterations, standardization, regularizationParam, elasticNetParam, tolerance);
  }

  @Override
  public Map<String, String> toMap() {
    return Params.putParams(new HashMap<String, String>(), maxIterations, standardization, regularizationParam,
                            elasticNetParam, tolerance);
  }
}
