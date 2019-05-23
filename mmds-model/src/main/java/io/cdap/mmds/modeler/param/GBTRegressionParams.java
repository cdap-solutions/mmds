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
import io.cdap.mmds.spec.ParamSpec;
import io.cdap.mmds.spec.Params;
import io.cdap.mmds.spec.StringParam;
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
    this.lossType = new StringParam("lossType", "Loss Type",
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
