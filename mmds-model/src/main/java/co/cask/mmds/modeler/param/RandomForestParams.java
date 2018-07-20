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
import co.cask.mmds.spec.StringParam;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;

/**
 * Modeler parameters for Random Forest algorithms.
 */
public class RandomForestParams extends TreeParams {
  private final IntParam numTrees;
  private final DoubleParam subsamplingRate;
  private final StringParam featureSubsetStrategy;

  public RandomForestParams(Map<String, String> modelParams, String defaultSubsetStrategy) {
    super(modelParams);
    this.numTrees = new IntParam("numTrees", "Num Trees",
                                 "Number of trees to train. " +
                                   "If 1, then no bootstrapping is used. If > 1, then bootstrapping is done.",
                                 20, new Range(1, true), modelParams);
    this.subsamplingRate = new DoubleParam("subsamplingRate", "Sub-sampling Rate",
                                           "Fraction of the training data used for learning each decision tree.",
                                           1.0d, new Range(0d, 1d, false, true), modelParams);
    this.featureSubsetStrategy = new StringParam("featureSubsetStrategy", "Feature Subset Strategy",
                                                 "The number of features to consider for splits at each tree node. " +
                                                   "'auto' chooses automatically for task: " +
                                                   "If numTrees == 1, sets to 'all.' " +
                                                   "If numTrees > 1 (forest), sets to 'sqrt' for classification and" +
                                                   "'onethird' for regression. " +
                                                   "'all' users all features. " +
                                                   "'onethird' uses 1/3 of the features. " +
                                                   "'sqrt' uses sqrt(number of features). " +
                                                   "'log2' uses log2(number of features). " +
                                                   "'n' when n is in the range (0, 1.0], use n * number of features. " +
                                                   "When n is in the range (1, number of features), uses n features.",
                                                 defaultSubsetStrategy,
                                                 ImmutableSet.of("auto", "all", "onethird", "sqrt", "log2", "n"),
                                                 modelParams);
  }

  public void setParams(org.apache.spark.ml.tree.RandomForestParams params) {
    super.setParams(params);
    params.setNumTrees(numTrees.getVal());
    params.setSubsamplingRate(subsamplingRate.getVal());
    params.setFeatureSubsetStrategy(featureSubsetStrategy.getVal());
  }

  @Override
  public Map<String, String> toMap() {
    return Params.putParams(super.toMap(), numTrees, subsamplingRate, featureSubsetStrategy);
  }

  @Override
  public List<ParamSpec> getSpec() {
    return Params.addParams(super.getSpec(), numTrees, subsamplingRate, featureSubsetStrategy);
  }
}
