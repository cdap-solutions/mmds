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
import io.cdap.mmds.spec.DoubleParam;
import io.cdap.mmds.spec.ParamSpec;
import io.cdap.mmds.spec.Params;
import io.cdap.mmds.spec.Range;
import io.cdap.mmds.spec.StringParam;
import org.apache.spark.ml.classification.LogisticRegression;

import java.util.List;
import java.util.Map;

/**
 * Modeler parameters for logistic regression.
 */
public class LogisticRegressionParams extends RegressionParams {
  private final DoubleParam threshold;
  private final StringParam family;

  public LogisticRegressionParams(Map<String, String> modelParams) {
    super(modelParams);
    // [0, 1]
    threshold = new DoubleParam("threshold", "Threshold",
                                "Threshold in binary classification. " +
                                  "If the estimated probability of class label 1 is greater than threshold, " +
                                  "then predict 1, else 0. " +
                                  "A high threshold encourages the model to predict 0 more often. " +
                                  "A low threshold encourages the model to predict 1 more often.",
                                0.5d, new Range(0d, 1d, true, true), modelParams);
    // auto, binomial, multinomial
    family = new StringParam("family", "Family",
                             "Label distribution to be used in the model. " +
                               "'auto' will automatically select the family based on the number of classes. " +
                               "If numClasses == 1 or numClasses == 2, sets to 'binomial'. " +
                               "Else, sets to 'multinomial'. " +
                               "'binomial' uses binary logistic regression with pivoting. " +
                               "'multinomial' uses multinomial logistic (softmax) regression without pivoting.",
                             "auto", ImmutableSet.of("auto", "binomial", "multinomial"), modelParams);
  }

  @Override
  public List<ParamSpec> getSpec() {
    return Params.addParams(super.getSpec(), threshold, family);
  }

  @Override
  public Map<String, String> toMap() {
    return Params.putParams(super.toMap(), threshold, family);
  }

  public void setParams(LogisticRegression modeler) {
    modeler.setMaxIter(maxIterations.getVal());
    modeler.setStandardization(standardization.getVal());
    modeler.setRegParam(regularizationParam.getVal());
    modeler.setElasticNetParam(elasticNetParam.getVal());
    modeler.setTol(tolerance.getVal());
    modeler.setThreshold(threshold.getVal());
    modeler.setFamily(family.getVal());
  }
}
