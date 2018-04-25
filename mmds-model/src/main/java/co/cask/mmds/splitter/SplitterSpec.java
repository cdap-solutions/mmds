package co.cask.mmds.splitter;

import co.cask.mmds.spec.ParamSpec;

import java.util.List;

/**
 * Splitter specification.
 */
public class SplitterSpec {
  private final String type;
  private final String label;
  private final List<ParamSpec> hyperparameters;

  public SplitterSpec(String type, String label, List<ParamSpec> hyperparameters) {
    this.type = type;
    this.label = label;
    this.hyperparameters = hyperparameters;
  }

  public String getType() {
    return type;
  }

  public String getLabel() {
    return label;
  }

  public List<ParamSpec> getHyperparameters() {
    return hyperparameters;
  }
}
