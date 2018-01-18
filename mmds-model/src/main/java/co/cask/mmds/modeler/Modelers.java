package co.cask.mmds.modeler;

import co.cask.mmds.api.Modeler;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Collection of built in Modelers.
 */
public class Modelers {
  private static final Map<String, Modeler> MODELERS = ImmutableMap.<String, Modeler>builder()
    .put(Algorithm.REGRESSION_DECISION_TREE.getId(), new DecisionTreeRegressionModeler())
    .put(Algorithm.REGRESSION_GENERALIZED_LINEAR.getId(), new GeneralizedLinearRegressionModeler())
    .put(Algorithm.REGRESSION_GRADIENT_BOOSTED_TREE.getId(), new GBTRegressionModeler())
    .put(Algorithm.REGRESSION_LINEAR.getId(), new LinearRegressionModeler())
    .put(Algorithm.REGRESSION_RANDOM_FOREST.getId(), new RandomForestRegressionModeler())
    .put(Algorithm.CLASSIFIER_DECISION_TREE.getId(), new DecisionTreeClassifierModeler())
    .put(Algorithm.CLASSIFIER_GRADIENT_BOOSTED_TREE.getId(), new GBTClassifierModeler())
    .put(Algorithm.CLASSIFIER_LOGISTIC_REGRESSION.getId(), new LogisticRegressionModeler())
    .put(Algorithm.CLASSIFIER_MULTILAYER_PERCEPTRON.getId(), new MultilayerPerceptronModeler())
    .put(Algorithm.CLASSIFIER_NAIVE_BAYES.getId(), new NaiveBayesModeler())
    .put(Algorithm.CLASSIFIER_RANDOM_FOREST.getId(), new RandomForestClassifierModeler())
    .build();

  @Nullable
  public static Modeler getModeler(String algorithm) {
    return MODELERS.get(algorithm);
  }
}
