package co.cask.mmds.modeler.param;

import co.cask.mmds.spec.BoolParam;
import co.cask.mmds.spec.ParamSpec;
import co.cask.mmds.spec.Params;
import co.cask.mmds.spec.StringParam;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.ml.regression.LinearRegression;

import java.util.List;
import java.util.Map;

/**
 * Modeler parameters for linear regression.
 */
public class LinearRegressionParams extends RegressionParams {
  private final BoolParam fitIntercept;
  private final StringParam solver;

  public LinearRegressionParams(Map<String, String> modelParams) {
    super(modelParams);
    fitIntercept = new BoolParam("fitIntercept", "Fit Intercept", "If the intercept should be fit", true, modelParams);
    // "l-bfgs", "normal" and "auto"
    solver = new StringParam("solver", "Solver",
                             "The solver algorithm used for optimization. " +
                               "'l-bfgs' uses Limited-memory BFGS, " +
                               "which is a limited-memory quasi-Newton optimization method. " +
                               "'normal' uses the Normal Equation as an analytical solution to the problem. " +
                               "'auto' (default) means that the solver algorithm is selected automatically. " +
                               "The Normal Equations solver will be used when possible, but will automatically fall"  +
                               "back to iterative optimization methods when needed.",
                             "auto", ImmutableSet.of("auto", "l-bfgs", "normal"), modelParams);
  }

  public void setParams(LinearRegression modeler) {
    modeler.setMaxIter(maxIterations.getVal());
    modeler.setStandardization(standardization.getVal());
    modeler.setRegParam(regularizationParam.getVal());
    modeler.setElasticNetParam(elasticNetParam.getVal());
    modeler.setTol(tolerance.getVal());
    modeler.setFitIntercept(fitIntercept.getVal());
    modeler.setSolver(solver.getVal());
  }

  @Override
  public List<ParamSpec> getSpec() {
    return Params.addParams(super.getSpec(), fitIntercept, solver);
  }

  @Override
  public Map<String, String> toMap() {
    return Params.putParams(super.toMap(), fitIntercept, solver);
  }
}
