package co.cask.mmds.manager.runner;

import co.cask.mmds.modeler.param.spec.ParamSpec;

import java.util.List;

/**
 * Describes an algorithm.
 */
public class AlgorithmSpec {
  private final String algorithm;
  private final List<ParamSpec> hyperparameters;

  public AlgorithmSpec(String algorithm, List<ParamSpec> hyperparameters) {
    this.algorithm = algorithm;
    this.hyperparameters = hyperparameters;
  }
}
