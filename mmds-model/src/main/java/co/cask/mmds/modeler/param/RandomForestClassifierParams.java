package co.cask.mmds.modeler.param;

import java.util.Map;

/**
 * Params for random forest classifiers.
 */
public class RandomForestClassifierParams extends RandomForestParams {

  public RandomForestClassifierParams(Map<String, String> modelParams) {
    super(modelParams, "sqrt");
  }
}
