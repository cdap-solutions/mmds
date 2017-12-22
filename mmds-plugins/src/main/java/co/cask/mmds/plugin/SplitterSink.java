package co.cask.mmds.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.api.spark.sql.DataFrames;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.cdap.etl.api.batch.SparkPluginContext;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.mmds.Constants;
import co.cask.mmds.data.ColumnSplitStats;
import co.cask.mmds.data.DataSplitStats;
import co.cask.mmds.data.DataSplitTable;
import co.cask.mmds.data.ExperimentMetaTable;
import co.cask.mmds.data.ExperimentStore;
import co.cask.mmds.data.ModelTable;
import co.cask.mmds.data.SplitKey;
import co.cask.mmds.stats.CategoricalHisto;
import co.cask.mmds.stats.NumericHisto;
import co.cask.mmds.stats.NumericStats;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Splits data.
 */
@Plugin(type = SparkSink.PLUGIN_TYPE)
@Name("DataSplitter")
@Description("Splits incoming data into two datasets, storing each as a data split.")
public class SplitterSink extends SparkSink<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SplitterSink.class);
  private final Conf conf;

  public SplitterSink(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void prepareRun(SparkPluginContext sparkPluginContext) throws Exception {
    // no-op
  }

  @Override
  public void run(SparkExecutionPluginContext context, JavaRDD<StructuredRecord> javaRDD) throws Exception {
    Schema inputSchema = conf.getSchema(context.getInputSchema());
    if (inputSchema == null) {
      // should never happen, checked at configure time
      throw new IllegalStateException("Null (unknown) input schema for model trainer.");
    }
    SQLContext sqlContext = new SQLContext(context.getSparkContext().sc());


    Table modelMeta = context.getDataset(Constants.Dataset.MODEL_META);
    Table experiments = context.getDataset(Constants.Dataset.EXPERIMENTS_META);
    PartitionedFileSet splits = context.getDataset(Constants.Dataset.SPLITS);
    DataSplitTable dataSplitTable = new DataSplitTable(splits);
    ExperimentStore store = new ExperimentStore(
      new ExperimentMetaTable(experiments), dataSplitTable, new ModelTable(modelMeta));

    SplitKey key = new SplitKey(conf.getExperimentId(), conf.getSplitId());
    DataSplitStats splitStats = store.getSplit(key);
    if (splitStats == null) {
      throw new IllegalArgumentException(String.format("Split '%s' in Experiment '%s' does not exist.",
                                                       conf.getSplitId(), conf.getExperimentId()));
    }

    // convert StructuredRecord into Row
    StructType rowType = DataFrames.toDataType(inputSchema);
    JavaRDD<Row> rowRDD = javaRDD.map(new RecordToRow(rowType));

    // convert RDD to DataFrame
    Dataset<Row> rawData = sqlContext.createDataFrame(rowRDD, rowType).cache();

    long start = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

    double[] splitWeights = new double[] { 100 - conf.getTestSplitPercentage(), conf.getTestSplitPercentage() };
    Dataset<Row>[] split = rawData.randomSplit(splitWeights);
    Dataset<Row> trainingSplit = split[0].cache();
    Dataset<Row> testSplit = split[1].cache();

    Location splitLocation = dataSplitTable.getLocation(key);

    Location trainingLocation = splitLocation.append("train");
    Location testLocation = splitLocation.append("test");

    String trainingPath = trainingLocation.toURI().getPath();
    String testPath = testLocation.toURI().getPath();

    trainingSplit.write().mode(SaveMode.Overwrite).format("parquet").save(trainingPath);
    testSplit.write().mode(SaveMode.Overwrite).format("parquet").save(testPath);

    long splitEnd = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    LOG.info("Time to split = {} seconds, {} minutes",
             splitEnd - start, TimeUnit.MINUTES.convert(splitEnd - start, TimeUnit.SECONDS));

    List<ColumnSplitStats> stats = getStats(trainingSplit, testSplit, inputSchema);
    long statsEnd = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    LOG.info("Time to get stats = {} seconds, {} minutes",
             statsEnd - splitEnd, TimeUnit.MINUTES.convert(statsEnd - splitEnd, TimeUnit.SECONDS));

    store.finishSplit(key, trainingPath, testPath, stats);
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
    LOG.info("Time to get categorical stats = {} seconds, {} minutes",
             catEnd - start, TimeUnit.MINUTES.convert(catEnd - start, TimeUnit.SECONDS));

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

      Double trainMin = trainStats.getMin();
      Double testMin = testStats.getMin();
      Double min;
      if (trainMin == null && testMin == null) {
        min = null;
      } else if (trainMin == null) {
        min = testMin;
      } else if (testMin == null) {
        min = trainMin;
      } else {
        min = Math.min(trainMin, testMin);
      }

      Double trainMax = trainStats.getMax();
      Double testMax = testStats.getMax();
      Double max;
      if (trainMax == null && testMax == null) {
        max = null;
      } else if (trainMax == null) {
        max = testMax;
      } else if (testMax == null) {
        max = trainMax;
      } else {
        max = Math.max(trainMax, testMax);
      }

      columnMinMax.put(column, new Tuple2<>(min, max));
    }

    long numericEnd = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    LOG.info("Time to get numeric stats, 1st pass = {} seconds, {} minutes",
             numericEnd - catEnd, TimeUnit.MINUTES.convert(numericEnd - catEnd, TimeUnit.SECONDS));

    // generate bins from min, max
    Map<String, NumericHisto> trainNumericHistos = trainNumericValues.mapToPair(new ToNumericHisto(columnMinMax))
      .reduceByKey(NumericHisto::merge, numericColumns.size())
      .collectAsMap();
    Map<String, NumericHisto> testNumericHistos = testNumericValues.mapToPair(new ToNumericHisto(columnMinMax))
      .reduceByKey(NumericHisto::merge, numericColumns.size())
      .collectAsMap();

    long numericStatsEnd = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    LOG.info("Time to get numeric stats, 2nd pass = {} seconds, {} minutes",
             numericStatsEnd - numericEnd, TimeUnit.MINUTES.convert(numericStatsEnd - numericEnd, TimeUnit.SECONDS));

    for (Map.Entry<String, NumericHisto> entry : trainNumericHistos.entrySet()) {
      String columnName = entry.getKey();
      NumericHisto trainHisto = entry.getValue();
      NumericHisto testHisto = testNumericHistos.get(columnName);
      stats.add(new ColumnSplitStats(columnName, trainHisto, testHisto));
    }

    return stats;
  }

  /**
   * Conf for Model Trainer SparkSink
   */
  public static class Conf extends PluginConfig {

    @Macro
    private String experimentId;

    @Macro
    private String splitId;

    @Macro
    @Nullable
    @Description("What percentage of the input data should be used as test data, specified as an integer. " +
      "Defaults to 10.")
    private Integer testSplitPercentage;

    @Macro
    @Nullable
    @Description("The input schema of the data. This is only required if the input schema is not known at " +
      "deploy time. This is commonly the case if the schema is a macro.")
    private String schema;

    @Macro
    @Nullable
    private String splitDataset;

    // to set default values
    public Conf() {
      testSplitPercentage = 10;
      splitDataset = "experiment_splits";
    }

    private String getExperimentId() {
      return experimentId;
    }

    private String getSplitId() {
      return splitId;
    }

    private String getSplitDataset() {
      return splitDataset;
    }

    private Integer getTestSplitPercentage() {
      return testSplitPercentage;
    }

    private Schema getSchema(@Nullable Schema inputSchema) {
      if (inputSchema != null) {
        return inputSchema;
      }

      if (schema == null) {
        if (containsMacro("schema")) {
          return null;
        }
        throw new IllegalArgumentException("The input schema could not be determined from the incoming stage during " +
                                             "the pipeline deployment process, " +
                                             "so you must set the schema property for the plugin.");
      }
      try {
        return Schema.parseJson(schema);
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage());
      }
    }

  }
}
