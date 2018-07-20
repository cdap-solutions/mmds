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
