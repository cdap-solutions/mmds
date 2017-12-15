package co.cask.mmds.modeler.param;

import co.cask.mmds.modeler.param.spec.DoubleParam;
import co.cask.mmds.modeler.param.spec.IntParam;
import co.cask.mmds.modeler.param.spec.ParamSpec;
import co.cask.mmds.modeler.param.spec.Params;
import co.cask.mmds.modeler.param.spec.Range;
import co.cask.mmds.modeler.param.spec.StringParam;
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
    this.numTrees = new IntParam("numTrees",
                                 "Number of trees to train. " +
                                   "If 1, then no bootstrapping is used. If > 1, then bootstrapping is done.",
                                 20, new Range(1, true), modelParams);
    this.subsamplingRate = new DoubleParam("subsamplingRate",
                                           "Fraction of the training data used for learning each decision tree.",
                                           1.0d, new Range(0d, 1d, false, true), modelParams);
    this.featureSubsetStrategy = new StringParam("featureSubsetStrategy",
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
