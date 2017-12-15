package co.cask.mmds.modeler.param;

import co.cask.mmds.modeler.param.spec.DoubleParam;
import co.cask.mmds.modeler.param.spec.ParamSpec;
import co.cask.mmds.modeler.param.spec.Params;
import co.cask.mmds.modeler.param.spec.Range;
import co.cask.mmds.modeler.param.spec.StringParam;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.ml.classification.NaiveBayes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Modeler parameters for Naive Bayes.
 */
public class NaiveBayesParams implements ModelerParams {
  private final DoubleParam smoothing;
  private final StringParam type;

  public NaiveBayesParams(Map<String, String> modelParams) {
    smoothing = new DoubleParam("smoothing", "smoothing parameter", 1.0d, new Range(0, true), modelParams);
    type = new StringParam("type", "model type", "multinomial",
                           ImmutableSet.of("multinomial", "bernoulli"), modelParams);
  }

  public void setParams(NaiveBayes modeler) {
    modeler.setSmoothing(smoothing.getVal());
    modeler.setModelType(type.getVal());
  }

  @Override
  public Map<String, String> toMap() {
    return Params.putParams(new HashMap<String, String>(), smoothing, type);
  }

  @Override
  public List<ParamSpec> getSpec() {
    return Params.addParams(new ArrayList<ParamSpec>(), smoothing, type);
  }
}
