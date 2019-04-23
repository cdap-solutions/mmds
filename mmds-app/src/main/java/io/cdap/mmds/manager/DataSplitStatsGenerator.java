/*
 * Copyright © 2017-2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.mmds.manager;

import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.spark.service.SparkHttpServicePluginContext;
import co.cask.cdap.api.spark.sql.DataFrames;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import com.google.common.base.Joiner;
import io.cdap.mmds.NullableMath;
import io.cdap.mmds.data.ColumnSplitStats;
import io.cdap.mmds.data.DataSplitInfo;
import io.cdap.mmds.splitter.DataSplitResult;
import io.cdap.mmds.splitter.DatasetSplitter;
import io.cdap.mmds.splitter.ToCatHisto;
import io.cdap.mmds.splitter.ToDoubleValues;
import io.cdap.mmds.splitter.ToNumericHisto;
import io.cdap.mmds.stats.CategoricalHisto;
import io.cdap.mmds.stats.NumericHisto;
import io.cdap.mmds.stats.NumericStats;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Splits data.
 */
public class DataSplitStatsGenerator implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(DataSplitStatsGenerator.class);
  private final SparkSession sparkSession;
  private final DatasetSplitter splitter;
  private final SparkHttpServicePluginContext pluginContext;
  private final ServiceDiscoverer serviceDiscoverer;
  private final PipelineConfigurer pipelineConfigurer;

  public DataSplitStatsGenerator(SparkSession sparkSession, DatasetSplitter splitter,
                                 SparkHttpServicePluginContext pluginContext, ServiceDiscoverer serviceDiscoverer) {
    this.sparkSession = sparkSession;
    this.splitter = splitter;
    this.pluginContext = pluginContext;
    this.serviceDiscoverer = serviceDiscoverer;
    this.pipelineConfigurer = new WranglerPipelineConfigurer(pluginContext);
  }

  public DataSplitResult split(DataSplitInfo dataSplitInfo) throws IOException {

    PluginProperties wranglerProperties = PluginProperties.builder()
      .add("schema", dataSplitInfo.getDataSplit().getSchema().toString())
      .add("field", "*")
      .add("directives", Joiner.on("\n").join(dataSplitInfo.getDataSplit().getDirectives()))
      .add("threshold", "-1")
      .add("precondition", "false")
      .build();
    Transform wrangler = pluginContext.usePlugin(Transform.PLUGIN_TYPE, "Wrangler", "wrangler", wranglerProperties);
    if (wrangler == null) {
      throw new IllegalStateException("Could not find wrangler plugin. " +
                                        "Please make sure it has been deployed with MMDS as a parent.");
    }
    // configure to let it validate directives and register UDDs
    wrangler.configurePipeline(pipelineConfigurer);

    Schema schema = dataSplitInfo.getDataSplit().getSchema();
    JavaRDD<Row> rowRDD = sparkSession.read()
      .format("text")
      .load(dataSplitInfo.getExperiment().getSrcpath())
      .javaRDD()
      .flatMap(new WranglerFunction(schema, pluginContext, serviceDiscoverer));

    StructType rowType = DataFrames.toDataType(schema);

    // convert RDD to DataFrame
    Dataset<Row> rawData = sparkSession.createDataFrame(rowRDD, rowType).cache();

    long start = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

    Dataset<Row>[] split = splitter.split(rawData, dataSplitInfo.getDataSplit().getParams());
    Dataset<Row> trainingSplit = split[0].cache();
    Dataset<Row> testSplit = split[1].cache();

    Location splitLocation = dataSplitInfo.getSplitLocation();
    Location trainingLocation = splitLocation.append("train");
    Location testLocation = splitLocation.append("test");

    String trainingPath = trainingLocation.toURI().getPath();
    String testPath = testLocation.toURI().getPath();

    trainingSplit.write().mode(SaveMode.Overwrite).format("parquet").save(trainingPath);
    testSplit.write().mode(SaveMode.Overwrite).format("parquet").save(testPath);

    long splitEnd = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    LOG.info("Time to split = {} seconds", splitEnd - start);

    List<ColumnSplitStats> stats = getStats(trainingSplit, testSplit, dataSplitInfo.getDataSplit().getSchema());
    long statsEnd = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    LOG.info("Time to get stats = {} seconds", statsEnd - splitEnd);

    return new DataSplitResult(trainingPath, testPath, stats);
  }

  private List<ColumnSplitStats> getStats(Dataset<Row> train, Dataset<Row> test, Schema schema) {
    List<ColumnSplitStats> stats = new ArrayList<>(schema.getFields().size());

    List<Column> categoricalColumns = new ArrayList<>();
    List<String> categoricalNames = new ArrayList<>();
    List<Column> numericColumns = new ArrayList<>();
    List<String> numericNames = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Schema fieldSchema = field.getSchema();
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
      Schema.Type fieldType = fieldSchema.getType();

      Column col = new Column(fieldName);
      switch (fieldType) {
        case BOOLEAN:
          categoricalColumns.add(col.cast(DataTypes.StringType));
          categoricalNames.add(fieldName);
          break;
        case STRING:
          categoricalColumns.add(col);
          categoricalNames.add(fieldName);
          break;
        case INT:
        case LONG:
        case FLOAT:
          numericColumns.add(col.cast(DataTypes.DoubleType));
          numericNames.add(fieldName);
          break;
        case DOUBLE:
          numericColumns.add(col);
          numericNames.add(fieldName);
          break;
      }
    }

    int numCategorical = categoricalColumns.size();
    int numNumeric = numericColumns.size();
    Dataset<Row> trainCategoricalSplit = train.select(categoricalColumns.toArray(new Column[numCategorical]));
    Dataset<Row> testCategoricalSplit = test.select(categoricalColumns.toArray(new Column[numCategorical]));
    Dataset<Row> trainNumericSplit = train.select(numericColumns.toArray(new Column[numNumeric]));
    Dataset<Row> testNumericSplit = test.select(numericColumns.toArray(new Column[numNumeric]));

    long start = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

    Map<String, CategoricalHisto> trainCategoricalHistograms = trainCategoricalSplit.javaRDD()
      .flatMapToPair(new ToCatHisto(categoricalNames))
      .reduceByKey(CategoricalHisto::merge, categoricalColumns.size())
      .collectAsMap();
    Map<String, CategoricalHisto> testCategoricalHistograms = testCategoricalSplit.javaRDD()
      .flatMapToPair(new ToCatHisto(categoricalNames))
      .reduceByKey(CategoricalHisto::merge, categoricalColumns.size())
      .collectAsMap();

    long catEnd = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    LOG.info("Time to get categorical stats = {} seconds", catEnd - start);

    for (Map.Entry<String, CategoricalHisto> entry : trainCategoricalHistograms.entrySet()) {
      String columnName = entry.getKey();
      CategoricalHisto trainHisto = entry.getValue();
      CategoricalHisto testHisto = testCategoricalHistograms.get(columnName);
      stats.add(new ColumnSplitStats(columnName, trainHisto, testHisto));
    }

    // get min, max from numericStats
    JavaPairRDD<String, Double> trainNumericValues = trainNumericSplit.javaRDD()
      .flatMapToPair(new ToDoubleValues(numericNames));
    JavaPairRDD<String, Double> testNumericValues = testNumericSplit.javaRDD()
      .flatMapToPair(new ToDoubleValues(numericNames));

    Map<String, NumericStats> trainNumericStats = trainNumericValues
      .mapValues(NumericStats::new)
      .reduceByKey(NumericStats::merge, numNumeric)
      .collectAsMap();
    Map<String, NumericStats> testNumericStats = testNumericValues
      .mapValues(NumericStats::new)
      .reduceByKey(NumericStats::merge, numNumeric)
      .collectAsMap();

    Map<String, Tuple2<Double, Double>> columnMinMax = new HashMap<>();
    for (Map.Entry<String, NumericStats> entry : trainNumericStats.entrySet()) {
      String column = entry.getKey();
      NumericStats trainStats = entry.getValue();
      NumericStats testStats = testNumericStats.get(column);

      Double min = NullableMath.min(trainStats.getMin(), testStats.getMin());
      Double max = NullableMath.max(trainStats.getMax(), testStats.getMax());

      columnMinMax.put(column, new Tuple2<>(min, max));
    }

    long numericEnd = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    LOG.info("Time to get numeric stats, 1st pass = {} seconds", numericEnd - catEnd);

    // generate bins from min, max
    Map<String, NumericHisto> trainNumericHistos = trainNumericValues.mapToPair(new ToNumericHisto(columnMinMax))
      .reduceByKey(NumericHisto::merge, numericColumns.size())
      .collectAsMap();
    Map<String, NumericHisto> testNumericHistos = testNumericValues.mapToPair(new ToNumericHisto(columnMinMax))
      .reduceByKey(NumericHisto::merge, numericColumns.size())
      .collectAsMap();

    long numericStatsEnd = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    LOG.info("Time to get numeric stats, 2nd pass = {} seconds", numericStatsEnd - numericEnd);

    for (Map.Entry<String, NumericHisto> entry : trainNumericHistos.entrySet()) {
      String columnName = entry.getKey();
      NumericHisto trainHisto = entry.getValue();
      NumericHisto testHisto = testNumericHistos.get(columnName);
      stats.add(new ColumnSplitStats(columnName, trainHisto, testHisto));
    }

    return stats;
  }

  @Override
  public void close() throws Exception {
    pluginContext.close();
  }
}
