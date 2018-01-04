package co.cask.mmds.manager.splitter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Splits data randomly.
 */
public class RandomDatasetSplitter implements DatasetSplitter {
  private final double[] splitWeights;

  public RandomDatasetSplitter(double testPercentage) {
    this.splitWeights = new double[] { 100 - testPercentage, testPercentage };
  }

  @Override
  public Dataset<Row>[] split(Dataset<Row> data) {
    return data.randomSplit(splitWeights);
  }
}
