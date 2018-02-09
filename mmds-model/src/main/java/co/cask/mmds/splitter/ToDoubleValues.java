package co.cask.mmds.splitter;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Used to convert a collection of Rows into a collection of tuple2s, where the first element is a column name and the
 * second element is a value for that column.
 */
public class ToDoubleValues implements PairFlatMapFunction<Row, String, Double> {
  private final List<String> columns;

  public ToDoubleValues(List<String> columns) {
    this.columns = new ArrayList<>(columns);
  }

  @Override
  public Iterator<Tuple2<String, Double>> call(Row row) throws Exception {
    List<Tuple2<String, Double>> values = new ArrayList<>();
    for (String column : columns) {
      values.add(new Tuple2<>(column, (Double) row.getAs(column)));
    }
    return values.iterator();
  }
}
