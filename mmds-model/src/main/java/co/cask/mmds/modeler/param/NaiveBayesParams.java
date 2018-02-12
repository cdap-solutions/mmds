package co.cask.mmds.modeler.param;

import co.cask.mmds.spec.DoubleParam;
import co.cask.mmds.spec.ParamSpec;
import co.cask.mmds.spec.Parameters;
import co.cask.mmds.spec.Range;
import co.cask.mmds.spec.StringParam;
import com.google.common.collect.ImmutableSet;
import org.apache.spark.ml.classification.NaiveBayes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Modeler parameters for Naive Bayes.
 */
public class NaiveBayesParams implements Parameters {
  private final DoubleParam smoothing;
  private final StringParam type;

  public NaiveBayesParams(Map<String, String> modelParams) {
    smoothing = new DoubleParam("smoothing", "Smoothing", "smoothing parameter", 1.0d, new Range(0, true), modelParams);
    type = new StringParam("type", "Type", "model type", "multinomial",
                           ImmutableSet.of("multinomial", "bernoulli"), modelParams);
  }

  public void setParams(NaiveBayes modeler) {
    modeler.setSmoothing(smoothing.getVal());
    modeler.setModelType(type.getVal());
  }

  @Override
  public Map<String, String> toMap() {
    return co.cask.mmds.spec.Params.putParams(new HashMap<String, String>(), smoothing, type);
  }

  @Override
  public List<ParamSpec> getSpec() {
    return co.cask.mmds.spec.Params.addParams(new ArrayList<ParamSpec>(), smoothing, type);
  }
}
