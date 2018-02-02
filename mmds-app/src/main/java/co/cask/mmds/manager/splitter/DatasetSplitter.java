package co.cask.mmds.manager.splitter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A way to split spark Datasets.
 */
public interface DatasetSplitter {

  Dataset<Row>[] split(Dataset<Row> data);
}
