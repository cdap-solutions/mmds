/*
 * Copyright © 2017-2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.mmds.modeler.param;

import com.google.common.collect.ImmutableSet;
import io.cdap.mmds.spec.BoolParam;
import io.cdap.mmds.spec.ParamSpec;
import io.cdap.mmds.spec.Params;
import io.cdap.mmds.spec.StringParam;
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
    family = new StringParam("family", "Family", "The error distribution to be used in the model.",
                             "gaussian", ImmutableSet.of("gaussian", "binomial", "poisson", "gamma"), modelParams);
    // "identity", "log", "inverse", "logit", "probit", "cloglog" and "sqrt"
    link = new StringParam("link", "Link",
                           "Relationship between the linear predictor and the mean of the distribution function.",
                           "identity",
                           ImmutableSet.of("identity", "log", "inverse", "logit", "probit", "cloglog", "sqrt"),
                           modelParams);
    fitIntercept = new BoolParam("fitIntercept", "Fit Intercept", "If the intercept should be fit", true, modelParams);
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
