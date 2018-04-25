package co.cask.mmds.splitter;

import co.cask.mmds.splitter.param.RandomParams;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * Splits data randomly.
 */
public class RandomDatasetSplitter implements DatasetSplitter {

  @Override
  public SplitterSpec getSpec() {
    return new SplitterSpec("random", "random", new RandomParams(new HashMap<>()).getSpec());
  }

  @Override
  public RandomParams getParams(Map<String, String> params) {
    return new RandomParams(params);
  }

  @Override
  public Dataset<Row>[] split(Dataset<Row> data, Map<String, String> params) {
    RandomParams randomParams = getParams(params);
    Long seed = randomParams.getSeed();
    double[] weights = randomParams.getWeights();
    return seed == null ? data.randomSplit(weights) : data.randomSplit(weights, seed);
  }
}
