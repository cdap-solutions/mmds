package co.cask.mmds.splitter.param;

import co.cask.mmds.spec.IntParam;
import co.cask.mmds.spec.LongParam;
import co.cask.mmds.spec.ParamSpec;
import co.cask.mmds.spec.Parameters;
import co.cask.mmds.spec.Params;
import co.cask.mmds.spec.Range;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Parameters for random splitter.
 */
public class RandomParams implements Parameters {
  private final IntParam testPercentage;
  private final LongParam seed;

  public RandomParams(Map<String, String> properties) {
    this.testPercentage = new IntParam("testPercentage", "Test Percentage",
                                       "Percent of data that should be used for testing. " +
                                         "Must be a whole number and greater than 0 but less than 100.",
                                       20, new Range(1, 100, true, false), properties);
    this.seed = new LongParam("seed", "Seed",
                              "Seed to use for random splitting of data. The same seed, " +
                                "same test percentage, and same data will produce the same splits.",
                              null, null, properties);
  }

  public double[] getWeights() {
    return new double[] { 100 - testPercentage.getVal(), testPercentage.getVal() };
  }

  @Nullable
  public Long getSeed() {
    return seed.getVal();
  }

  @Override
  public Map<String, String> toMap() {
    Map<String, String> map = new HashMap<>();
    return Params.putParams(map, testPercentage, seed);
  }

  @Override
  public List<ParamSpec> getSpec() {
    List<ParamSpec> spec = new ArrayList<>();
    return Params.addParams(spec, testPercentage, seed);
  }
}
