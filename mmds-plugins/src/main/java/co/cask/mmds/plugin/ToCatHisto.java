package co.cask.mmds.plugin;

import co.cask.mmds.stats.CategoricalHisto;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Used to convert a collection of Rows into a collection of tuple2s, where the first element is a column name and the
 * second element in a histogram of values for that column.
 */
public class ToCatHisto implements PairFlatMapFunction<Row, String, CategoricalHisto> {
  private final List<String> columns;

  public ToCatHisto(List<String> columns) {
    this.columns = new ArrayList<>(columns);
  }

  @Override
  public Iterator<Tuple2<String, CategoricalHisto>> call(Row row) throws Exception {
    List<Tuple2<String, CategoricalHisto>> histograms = new ArrayList<>();
    for (String column : columns) {
      Map<String, Long> counts = new HashMap<>();
      String val = row.getAs(column);
      long nullCount = 1L;
      long emptyCount = 0L;
      if (val != null) {
        counts.put(val, 1L);
        nullCount = 0L;
        if (val.isEmpty()) {
          emptyCount = 1L;
        }
      }
      CategoricalHisto histo = new CategoricalHisto(1L, nullCount, emptyCount, counts);
      histograms.add(new Tuple2<>(column, histo));
    }
    return histograms.iterator();
  }
}
