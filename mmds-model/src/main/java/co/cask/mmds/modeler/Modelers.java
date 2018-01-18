package co.cask.mmds.modeler;

import co.cask.mmds.api.Modeler;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Collection of built in Modelers.
 */
public class Modelers {
  private static final List<Modeler> MODELERS = ImmutableList.of(
    new DecisionTreeRegressionModeler(),
    new GeneralizedLinearRegressionModeler(),
    new GBTRegressionModeler(),
    new LinearRegressionModeler(),
    new RandomForestRegressionModeler(),
    new DecisionTreeClassifierModeler(),
    new GBTClassifierModeler(),
    new LogisticRegressionModeler(),
    new MultilayerPerceptronModeler(),
    new NaiveBayesModeler(),
    new RandomForestClassifierModeler());
  private static final Map<String, Modeler> MODELER_MAP = MODELERS.stream().collect(
    Collectors.toMap(modeler -> modeler.getAlgorithm().getId(), modeler -> modeler));

  public static Collection<String> getAlgorithms() {
    return MODELER_MAP.keySet();
  }

  public static Collection<Modeler> getModelers() {
    return MODELERS;
  }

  @Nullable
  public static Modeler getModeler(String algorithm) {
    return MODELER_MAP.get(algorithm);
  }
}
