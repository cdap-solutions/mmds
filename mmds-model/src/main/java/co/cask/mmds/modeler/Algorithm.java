package co.cask.mmds.modeler;

/**
 * Modeling algorithms.
 */
public enum Algorithm {
  REGRESSION_LINEAR("linear.regression", "Linear Regression"),
  REGRESSION_GENERALIZED_LINEAR("generalized.linear.regression", "Generalized Linear Regression"),
  REGRESSION_DECISION_TREE("decision.tree.regression", "Decision Tree Regression"),
  REGRESSION_RANDOM_FOREST("random.forest.regression", "Random Forest Regression"),
  REGRESSION_GRADIENT_BOOSTED_TREE("gradient.boosted.tree.regression", "Gradient Boosted Tree Regression"),
  CLASSIFIER_DECISION_TREE("decision.tree.classifier", "Decision Tree Classifier"),
  CLASSIFIER_RANDOM_FOREST("random.forest.classifier", "Random Forest Classifier"),
  CLASSIFIER_GRADIENT_BOOSTED_TREE("gradient.boosted.tree.classifier", "Gradient Boosted Tree Classifier"),
  CLASSIFIER_MULTILAYER_PERCEPTRON("multilayer.perceptron.classifier", "Multi-Layer Perceptron Classifier"),
  CLASSIFIER_LOGISTIC_REGRESSION("logistic.regression", "Logistic Regression"),
  CLASSIFIER_NAIVE_BAYES("naive.bayes", "Naive Bayes");
  private final String id;
  private final String label;

  Algorithm(String id, String label) {
    this.id = id;
    this.label = label;
  }

  public String getId() {
    return id;
  }

  public String getLabel() {
    return label;
  }

  @Override
  public String toString() {
    return id;
  }
}
