package co.cask.mmds.manager;

import co.cask.mmds.api.AlgorithmType;
import co.cask.mmds.modeler.Algorithm;
import co.cask.mmds.modeler.param.spec.ParamSpec;

import java.util.List;

/**
 * Describes an algorithm.
 */
public class AlgorithmSpec {
  private final AlgorithmType type;
  private final String algorithm;
  private final String label;
  private final List<ParamSpec> hyperparameters;

  public AlgorithmSpec(Algorithm algorithm, List<ParamSpec> hyperparameters) {
    this.type = algorithm.getType();
    this.algorithm = algorithm.getId();
    this.label = algorithm.getLabel();
    this.hyperparameters = hyperparameters;
  }
}
