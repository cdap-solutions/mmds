package co.cask.mmds.splitter;

import co.cask.mmds.spec.Parameters;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

/**
 * A way to split spark Datasets.
 */
public interface DatasetSplitter {

  SplitterSpec getSpec();

  Parameters getParams(Map<String, String> params);

  Dataset<Row>[] split(Dataset<Row> data, Map<String, String> properties);
}
