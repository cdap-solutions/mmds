package co.cask.mmds.plugin;

import co.cask.mmds.stats.NumericHisto;
import co.cask.mmds.stats.NumericStats;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Map;

/**
 * Converts (column, value) to (column, histogram).
 */
public class ToNumericHisto implements PairFunction<Tuple2<String, Double>, String, NumericHisto> {
  private final Map<String, NumericStats> columnStats;

  public ToNumericHisto(Map<String, NumericStats> columnStats) {
    this.columnStats = columnStats;
  }

  @Override
  public Tuple2<String, NumericHisto> call(Tuple2<String, Double> input) throws Exception {
    NumericStats stats = columnStats.get(input._1());
    return new Tuple2<>(input._1(), new NumericHisto(stats.getMin(), stats.getMax(), 10, input._2()));
  }
}
