package co.cask.mmds.modeler.param;

import co.cask.mmds.modeler.param.spec.BoolParam;
import co.cask.mmds.modeler.param.spec.ParamSpec;
import co.cask.mmds.modeler.param.spec.Params;
import co.cask.mmds.modeler.param.spec.StringParam;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.ml.regression.GeneralizedLinearRegression;

import java.util.List;
import java.util.Map;

/**
 * Modeler parameters for linear regression.
 */
public class GeneralizedLinearRegressionParams extends RegressionParams {
  private final StringParam family;
  private final StringParam link;
  private final BoolParam fitIntercept;

  public GeneralizedLinearRegressionParams(Map<String, String> modelParams) {
    super(modelParams);
    // "gaussian", "binomial", "poisson" and "gamma"
    family = new StringParam("family", "The error distribution to be used in the model.",
                             "gaussian", ImmutableSet.of("gaussian", "binomial", "poisson", "gamma"), modelParams);
    // "identity", "log", "inverse", "logit", "probit", "cloglog" and "sqrt"
    link = new StringParam("link",
                           "Relationship between the linear predictor and the mean of the distribution function.",
                           "identity",
                           ImmutableSet.of("identity", "log", "inverse", "logit", "probit", "cloglog", "sqrt"),
                           modelParams);
    fitIntercept = new BoolParam("fitIntercept", "If the intercept should be fit", true, modelParams);
  }

  public void setParams(GeneralizedLinearRegression modeler) {
    modeler.setMaxIter(maxIterations.getVal());
    modeler.setRegParam(regularizationParam.getVal());
    modeler.setTol(tolerance.getVal());
    modeler.setFitIntercept(fitIntercept.getVal());
    modeler.setFamily(family.getVal());
    modeler.setLink(link.getVal());
  }

  @Override
  public Map<String, String> toMap() {
    return Params.putParams(super.toMap(), family, link, fitIntercept);
  }

  @Override
  public List<ParamSpec> getSpec() {
    return Params.addParams(super.getSpec(), family, link, fitIntercept);
  }
}
