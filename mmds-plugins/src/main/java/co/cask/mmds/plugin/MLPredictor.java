package co.cask.mmds.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.spark.sql.DataFrames;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkExecutionPluginContext;
import co.cask.mmds.Constants;
import co.cask.mmds.api.AlgorithmType;
import co.cask.mmds.api.Modeler;
import co.cask.mmds.data.ModelKey;
import co.cask.mmds.data.ModelMeta;
import co.cask.mmds.data.ModelTable;
import co.cask.mmds.modeler.Modelers;
import co.cask.mmds.modeler.feature.FeatureGenerator;
import co.cask.mmds.modeler.feature.FeatureGeneratorPredictor;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.PredictionModel;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Uses a deployed model to add a prediction field to incoming records.
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name("MLPredictor")
@Description("Uses a deployed model to add a prediction field to incoming records.")
public class MLPredictor extends SparkCompute<StructuredRecord, StructuredRecord> {
  private final PredictorConf conf;
  private String featuregenPath;
  private String modelPath;
  private String targetIndexPath;
  private Schema inputSchema;
  private Schema outputSchema;
  private Schema.Type predictionType;
  private FeatureGenerator featureGenerator;
  private Modeler modeler;

  public MLPredictor(PredictorConf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    if (inputSchema == null) {
      throw new IllegalArgumentException("ML Predictor cannot be used with a null input schema. " +
                                           "Please connect it to stages that have a set output schema.");
    }
    conf.validate(inputSchema);
    stageConfigurer.setOutputSchema(conf.getOutputSchema());
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    inputSchema = context.getInputSchema();
    // validate again in case there were macros
    conf.validate(inputSchema);
    outputSchema = conf.getOutputSchema();

    Schema predictionSchema = outputSchema.getField(conf.getPredictionField()).getSchema();
    predictionSchema = predictionSchema.isNullable() ? predictionSchema.getNonNullable() : predictionSchema;
    predictionType = predictionSchema.getType();

    // verify that the features used to train the model are present in the input.
    IndexedTable modelTable = context.getDataset(conf.getModelMetaDataset());
    ModelTable modelMetaTable = new ModelTable(modelTable);
    ModelKey key = new ModelKey(conf.getExperimentID(), conf.getModelID());
    ModelMeta meta = modelMetaTable.get(key);
    if (meta == null) {
      throw new IllegalArgumentException(String.format("Could not find model '%s' in experiment '%s'.",
                                                       conf.getModelID(), conf.getExperimentID()));
    }
    modeler = Modelers.getModeler(meta.getAlgorithm());
    if (modeler == null) {
      // should never happen
      throw new IllegalArgumentException(String.format("Model '%s' in experiment '%s' uses unknown algorithm '%s'",
                                                       conf.getModelID(), conf.getExperimentID(), meta.getAlgorithm()));
    }
    if (modeler.getAlgorithm().getType() == AlgorithmType.REGRESSION && predictionType == Schema.Type.STRING) {
      throw new IllegalArgumentException(
        String.format("Invalid getType for prediction field '%s'. " +
                        "Model '%s' in experiment '%s' is a regression model, " +
                        "which only supports double predictions.",
                      conf.getPredictionField(), conf.getModelID(), conf.getExperimentID()));
    }

    // validate that the same features used to train the model are present in the input.
    Set<String> featureSet = new HashSet<>(meta.getFeatures());
    Set<String> inputFields = new HashSet<>();
    for (Schema.Field field : inputSchema.getFields()) {
      inputFields.add(field.getName());
    }
    Set<String> missingFeatures = Sets.difference(featureSet, inputFields);
    if (!missingFeatures.isEmpty()) {
      throw new IllegalArgumentException(String.format("Input is missing feature fields %s.",
                                                       Joiner.on(',').join(missingFeatures)));
    }

    // validate that the actual files are there
    FileSet modelFiles = context.getDataset(conf.getModelDataset());
    featuregenPath = getComponentPath(modelFiles, Constants.Component.FEATUREGEN);
    if (featuregenPath == null) {
      throw new IllegalArgumentException(
        String.format("Could not find feature generation data for model '%s' in experiment '%s'. " +
                        "Please verify that the same model and model meta datasets used to train " +
                        "the model are used here.", conf.getModelID(), conf.getExperimentID()));
    }
    featureGenerator = new FeatureGeneratorPredictor(meta.getFeatures(), meta.getCategoricalFeatures(), featuregenPath);

    modelPath = getComponentPath(modelFiles, Constants.Component.MODEL);
    if (modelPath == null) {
      throw new IllegalArgumentException(
        String.format("Could not find the files for model '%s' in experiment '%s'. " +
                        "Please verify that the model was successfully trained.",
                      conf.getModelID(), conf.getExperimentID()));
    }

    targetIndexPath = getComponentPath(modelFiles, Constants.Component.TARGET_INDICES);
    if (targetIndexPath == null && modeler.getAlgorithm().getType() == AlgorithmType.CLASSIFICATION &&
      predictionType == Schema.Type.STRING) {
      throw new IllegalArgumentException(
        String.format("Could not find target index data for model '%s' in experiment '%s'. " +
                        "Please change the prediction field type to double.",
                      conf.getModelID(), conf.getExperimentID()));
    }
  }

  @Override
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext sparkExecutionPluginContext,
                                             JavaRDD<StructuredRecord> javaRDD) throws Exception {
    PredictionModel<Vector, ?> model = modeler.loadPredictor(modelPath);

    // convert StructuredRecord into Row
    StructType rowType = DataFrames.toDataType(inputSchema);
    JavaRDD<Row> rowRDD = javaRDD.map(new RecordToRow(rowType));

    // convert RDD to DataFrame
    SQLContext sqlContext = new SQLContext(sparkExecutionPluginContext.getSparkContext().sc());
    Dataset<Row> rawData = sqlContext.createDataFrame(rowRDD, rowType);

    // keep around fields in the output schema that are not features and not the prediction field
    Set<String> featureSet = new HashSet<>(featureGenerator.getFeatures());
    List<String> extraFields = new ArrayList<>();
    for (Schema.Field outputField : outputSchema.getFields()) {
      String outputFieldName = outputField.getName();
      if (!conf.getPredictionField().equals(outputFieldName) && !featureSet.contains(outputFieldName)) {
        extraFields.add(outputFieldName);
      }
    }

    // generate features
    Dataset<Row> featureData = featureGenerator.generateFeatures(rawData, extraFields);
    // make predictions
    Dataset predictions = model.transform(featureData);
    predictions.show();

    if (modeler.getAlgorithm().getType() == AlgorithmType.CLASSIFICATION && predictionType == Schema.Type.STRING) {
      StringIndexerModel indexerModel = StringIndexerModel.load(targetIndexPath);
      String[] labels = indexerModel.labels();
      IndexToString reverseIndex = new IndexToString()
        .setLabels(labels)
        .setInputCol(Constants.TRAINER_PREDICTION_FIELD)
        .setOutputCol(conf.getPredictionField());
      predictions = reverseIndex.transform(predictions);
    } else {
      predictions = predictions.withColumnRenamed(Constants.TRAINER_PREDICTION_FIELD, conf.getPredictionField());
    }

    // select just the fields we need, in the order we expect
    Column[] cols = new Column[outputSchema.getFields().size()];
    int i = 0;
    for (Schema.Field outputField : outputSchema.getFields()) {
      cols[i] = new Column(outputField.getName());
      i++;
    }
    predictions = predictions.select(cols);

    JavaRDD<StructuredRecord> output = predictions.toJavaRDD().map(new RowToRecord(outputSchema));
    return output;
  }

  @Nullable
  private String getComponentPath(FileSet modelFiles, String component) throws IOException {
    return modelFiles.getLocation(conf.getExperimentID())
      .append(conf.getModelID())
      .append(component)
      .toURI().getPath();
  }
}
