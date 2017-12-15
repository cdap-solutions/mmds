package co.cask.mmds.modeler;

/**
 * Modeling algorithms.
 */
public enum Algorithm {
  REGRESSION_LINEAR("linear.regression"),
  REGRESSION_GENERALIZED_LINEAR("generalized.linear.regression"),
  REGRESSION_DECISION_TREE("decision.tree.regression"),
  REGRESSION_RANDOM_FOREST("random.forest.regression"),
  REGRESSION_GRADIENT_BOOSTED_TREE("gradient.boosted.tree.regression"),
  CLASSIFIER_DECISION_TREE("decision.tree.classifier"),
  CLASSIFIER_RANDOM_FOREST("random.forest.classifier"),
  CLASSIFIER_GRADIENT_BOOSTED_TREE("gradient.boosted.tree.classifier"),
  CLASSIFIER_MULTILAYER_PERCEPTRON("multilayer.perceptron.classifier"),
  CLASSIFIER_LOGISTIC_REGRESSION("logistic.regression"),
  CLASSIFIER_NAIVE_BAYES("naive.bayes");
  private final String id;

  Algorithm(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  @Override
  public String toString() {
    return id;
  }
}
