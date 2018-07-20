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

package co.cask.mmds.modeler.train;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.mmds.Constants;
import co.cask.mmds.api.AlgorithmType;
import co.cask.mmds.api.Modeler;
import co.cask.mmds.data.EvaluationMetrics;
import co.cask.mmds.data.ModelTrainerInfo;
import co.cask.mmds.modeler.Modelers;
import co.cask.mmds.modeler.feature.FeatureGeneratorTrainer;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.PredictionModel;
import org.apache.spark.ml.Predictor;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.util.MLWritable;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Trains and saves a model.
 */
public class ModelTrainer {
  private static final Logger LOG = LoggerFactory.getLogger(ModelTrainer.class.getName());
  private final String algorithm;
  private final String outcomeField;
  private final Schema.Type outcomeType;
  private final Map<String, String> trainingParams;
  private final List<String> featureNames;
  private final Set<String> categoricalFeatures;
  private final Schema schema;

  public ModelTrainer(ModelTrainerInfo modelTrainerInfo) {
    algorithm = modelTrainerInfo.getModel().getAlgorithm();
    trainingParams = ImmutableMap.copyOf(modelTrainerInfo.getModel().getHyperparameters());
    schema = modelTrainerInfo.getDataSplitStats().getSchema();

    outcomeField = modelTrainerInfo.getExperiment().getOutcome();
    outcomeType = Schema.Type.valueOf(modelTrainerInfo.getExperiment().getOutcomeType().toUpperCase());
    featureNames = new ArrayList<>();
    categoricalFeatures = new HashSet<>();

    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      if (fieldName.equals(outcomeField)) {
        continue;
      }
      featureNames.add(fieldName);
      Schema fieldSchema = field.getSchema();
      Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      if (isCategorical(fieldType)) {
        categoricalFeatures.add(fieldName);
      }
    }
  }

  private boolean isCategorical(Schema.Type type) {
    return type == Schema.Type.STRING || type == Schema.Type.BOOLEAN;
  }

  public ModelOutput train(Dataset<Row> training, Dataset<Row> test) throws IOException {
    Dataset<Row> rawTraining = training.na().drop(new String[] { outcomeField });
    Dataset<Row> rawTest = test.na().drop(new String[] { outcomeField });

    // generate features
    LOG.info("Generating features for training and test data.");
    FeatureGeneratorTrainer featureGenerator = new FeatureGeneratorTrainer(featureNames, categoricalFeatures);
    Dataset trainingFeatures = featureGenerator.generateFeatures(rawTraining, outcomeField);
    LOG.info("Training features successfully generated.");
    Dataset testFeatures = featureGenerator.generateFeatures(rawTest, outcomeField);
    LOG.info("Test features successfully generated.");

    // if the outcome is categorical, index the values
    String finalOutcomeField = outcomeField;
    StringIndexerModel targetIndexModel = null;
    boolean isCategoricalOutput = isCategorical(outcomeType);
    String numericPredictionField = Constants.TRAINER_PREDICTION_FIELD;
    if (isCategoricalOutput) {
      String strOutcomeField = outcomeField;
      // StringIndexer doesn't do boolean, have to turn them into strings
      if (outcomeType == Schema.Type.BOOLEAN) {
        strOutcomeField = "_c_" + outcomeField;
        Column outcomeAsStr = new Column(outcomeField).cast(DataTypes.StringType);
        trainingFeatures = trainingFeatures.withColumn(strOutcomeField, outcomeAsStr);
        testFeatures = testFeatures.withColumn(strOutcomeField, outcomeAsStr);
      }

      finalOutcomeField = "_t_" + outcomeField;

      StringIndexer targetIndexer = new StringIndexer()
        .setInputCol(strOutcomeField)
        .setOutputCol(finalOutcomeField);
      targetIndexModel = targetIndexer.fit(trainingFeatures);
      trainingFeatures = targetIndexModel.transform(trainingFeatures);
      testFeatures = targetIndexModel.transform(testFeatures);
      numericPredictionField = "_n_" + numericPredictionField;
    }

    Modeler modeler = Modelers.getModeler(algorithm);
    Predictor<Vector, ?, ? extends PredictionModel> predictor = modeler.createPredictor(trainingParams);
    predictor.setLabelCol(finalOutcomeField);
    predictor.setFeaturesCol(Constants.FEATURES_FIELD);
    predictor.setPredictionCol(numericPredictionField);

    LOG.info("Training model...");
    PredictionModel model = predictor.fit(trainingFeatures);
    LOG.info("Model successfully trained.");
    LOG.info("Generating predictions on test data.");
    Dataset predictions = model.transform(testFeatures);
    LOG.info("Predictions successfully generated.");

    // reverse map categorical predictions so that they have the original value and not some number
    // that nobody knows how to interpret
    if (isCategorical(outcomeType)) {
      String[] labels = targetIndexModel.labels();
      IndexToString reverseIndex = new IndexToString()
        .setLabels(labels)
        .setInputCol(numericPredictionField)
        .setOutputCol(Constants.TRAINER_PREDICTION_FIELD);
      predictions = reverseIndex.transform(predictions);
    }

    LOG.info("Calculating evaluation metrics...");
    RDD<Tuple2<Object, Object>> predictionAndLabels =
      predictions.select(new Column(numericPredictionField),
                         new Column(finalOutcomeField).cast(DataTypes.DoubleType))
        .toJavaRDD()
        .map(new PredictionLabelFunction()).rdd();
    EvaluationMetrics evaluationMetrics;
    try {
      if (modeler.getAlgorithm().getType() == AlgorithmType.REGRESSION) {
        RegressionMetrics metrics = new RegressionMetrics(predictionAndLabels, false);
        double rmse = metrics.rootMeanSquaredError();
        double r2 = metrics.r2();
        double mae = metrics.meanAbsoluteError();
        double explainedVariance = metrics.explainedVariance();
        LOG.info("root mean squared error = {}, r2 = {}, mean absolute error = {}, explained variance = {}",
                 rmse, r2, mae, explainedVariance);
        evaluationMetrics = new EvaluationMetrics(rmse, r2, explainedVariance, mae);
      } else {
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels);
        double precision = metrics.weightedPrecision();
        double recall = metrics.weightedRecall();
        double f1 = metrics.weightedFMeasure();
        LOG.info("precision = {}, recall = {}, f1 = {}", precision, recall, f1);
        evaluationMetrics = new EvaluationMetrics(precision, recall, f1);
      }
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Failed to get evaluation metrics for the model. " +
                                   "Please check the logs for warnings or errors related to training problems. " +
                                   "If there were training problems, please check that your features are the correct " +
                                   "type. String features should represent categories, with multiple records for " +
                                   "each category value. For example, an ID should not be used as a feature, " +
                                   "as there is a unique value for each record.", e);
    }

    Column[] columns = new Column[schema.getFields().size() + 1];
    columns[0] = new Column(Constants.TRAINER_PREDICTION_FIELD);
    int i = 1;
    for (Schema.Field field : schema.getFields()) {
      columns[i] = new Column(field.getName());
      i++;
    }
    Dataset predictionsClean = predictions.select(columns);

    return ModelOutput.builder()
      .setTargetIndexModel(targetIndexModel)
      .setFeatureGenModel(featureGenerator.getFeatureGenModel())
      .setModel((MLWritable) model)
      .setEvaluationMetrics(evaluationMetrics)
      .setFeatureNames(featureNames)
      .setCategoricalFeatures(categoricalFeatures)
      .setPredictions(predictionsClean)
      .setAlgorithmType(modeler.getAlgorithm().getType())
      .setSchema(schema)
      .build();
  }

  /**
   * Transforms a Row with two fields - prediction and label, into a Tuple2. This is it's own class so that
   * the ModelTrainer doesn't have to be serializable.
   */
  private static class PredictionLabelFunction implements Function<Row, Tuple2> {

    @Override
    public Tuple2 call(Row row) throws Exception {
      return new Tuple2<>(row.get(0), row.get(1));
    }
  }
}
