package co.cask.mmds.splitter;

import co.cask.mmds.stats.NumericHisto;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Map;

/**
 * Converts (column, value) to (column, histogram).
 */
public class ToNumericHisto implements PairFunction<Tuple2<String, Double>, String, NumericHisto> {
  private final Map<String, Tuple2<Double, Double>> columnMinMax;

  public ToNumericHisto(Map<String, Tuple2<Double, Double>> columnMinMax) {
    this.columnMinMax = columnMinMax;
  }

  @Override
  public Tuple2<String, NumericHisto> call(Tuple2<String, Double> input) throws Exception {
    Tuple2<Double, Double> minMax = columnMinMax.get(input._1());
    return new Tuple2<>(input._1(), new NumericHisto(minMax._1(), minMax._2(), 10, input._2()));
  }
}
