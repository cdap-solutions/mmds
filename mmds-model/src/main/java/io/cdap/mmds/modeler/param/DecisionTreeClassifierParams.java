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
import org.apache.spark.ml.tree.DecisionTreeParams;
import org.apache.spark.ml.tree.DecisionTreeRegressorParams;

import java.util.List;
import java.util.Map;

/**
 * Common modeler parameters for tree based algorithms.
 */
public class DecisionTreeClassifierParams extends TreeParams {
  private StringParam impurity;

  public DecisionTreeClassifierParams(Map<String, String> modelParams) {
    super(modelParams);
    this.impurity = new StringParam("impurity", "Impurity", "Criterion used for information gain calculation", "gini",
                                    ImmutableSet.of("gini", "entropy"), modelParams);
  }

  @Override
  public List<ParamSpec> getSpec() {
    return Params.addParams(super.getSpec(), impurity);
  }

  @Override
  public Map<String, String> toMap() {
    return Params.putParams(super.toMap(), impurity);
  }

  public void setParams(DecisionTreeRegressorParams params) {
    setParams((DecisionTreeParams) params);
    params.setImpurity(impurity.getVal());
  }
}
