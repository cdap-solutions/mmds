package co.cask.mmds.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.spark.sql.DataFrames;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

/**
 * Converts a dataframe Row to a StructuredRecord.
 */
public class RowToRecord implements Function<Row, StructuredRecord> {
  private final Schema schema;

  public RowToRecord(Schema schema) {
    this.schema = schema;
  }

  @Override
  public StructuredRecord call(Row row) throws Exception {
    return DataFrames.fromRow(row, schema);
  }
}