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

import io.cdap.mmds.spec.*;
import io.cdap.mmds.spec.BoolParam;
import io.cdap.mmds.spec.DoubleParam;
import io.cdap.mmds.spec.IntParam;
import io.cdap.mmds.spec.ParamSpec;
import io.cdap.mmds.spec.Parameters;
import io.cdap.mmds.spec.Params;
import io.cdap.mmds.spec.Range;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Common modeler parameters for regression based algorithms.
 */
public abstract class RegressionParams implements Parameters {
  protected final IntParam maxIterations;
  protected final BoolParam standardization;
  protected final DoubleParam regularizationParam;
  protected final DoubleParam elasticNetParam;
  protected final DoubleParam tolerance;

  protected RegressionParams(Map<String, String> modelParams) {
    maxIterations = new IntParam("maxIterations", "Max Iterations", "max number of iterations",
                                 100, new Range(1, true), modelParams);
    standardization = new BoolParam("standardization", "Standardization",
                                    "Whether to standardize the training features before fitting the model.",
                                    true, modelParams);
    regularizationParam = new DoubleParam("regularizationParam", "Regularization Param", "regularization parameter",
                                          0.0d, new Range(0d, true), modelParams);
    elasticNetParam = new DoubleParam("elasticNetParam", "Elastic Net Param",
                                      "The ElasticNet mixing parameter. " +
                                        "For alpha = 0, the penalty is an L2 penalty. " +
                                        "For alpha = 1, it is an L1 penalty. " +
                                        "For alpha in (0,1), the penalty is a combination of L1 and L2.",
                                      0.0d, new Range(0d, 1d, true, true), modelParams);
    tolerance = new DoubleParam("tolerance", "Tolerance",
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
    return Params.putParams(new HashMap<>(), maxIterations, standardization, regularizationParam,
                            elasticNetParam, tolerance);
  }
}
