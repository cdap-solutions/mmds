package co.cask.mmds.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.spark.sql.DataFrames;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Function to map from {@link StructuredRecord} to {@link Row}.
 */
public class RecordToRow implements Function<StructuredRecord, Row> {
  private final StructType rowType;

  RecordToRow(StructType rowType) {
    this.rowType = rowType;
  }

  @Override
  public Row call(StructuredRecord record) throws Exception {
    return DataFrames.toRow(record, rowType);
  }
}
