package co.cask.mmds.modeler.param;

import co.cask.mmds.modeler.param.spec.DoubleParam;
import co.cask.mmds.modeler.param.spec.ParamSpec;
import co.cask.mmds.modeler.param.spec.Params;
import co.cask.mmds.modeler.param.spec.Range;
import co.cask.mmds.modeler.param.spec.StringParam;
import com.google.common.collect.ImmutableSet;
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
