package co.cask.mmds.modeler;

import co.cask.mmds.api.AlgorithmType;

/**
 * Modeling algorithms.
 */
public class Algorithm {
  private final AlgorithmType type;
  private final String id;
  private final String label;

  public Algorithm(AlgorithmType type, String id, String label) {
    this.type = type;
    this.id = id;
    this.label = label;
  }

  public AlgorithmType getType() {
    return type;
  }

  public String getId() {
    return id;
  }

  public String getLabel() {
    return label;
  }

}
