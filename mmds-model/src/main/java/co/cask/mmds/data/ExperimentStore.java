package co.cask.mmds.data;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.mmds.api.Modeler;
import co.cask.mmds.modeler.Modelers;
import co.cask.mmds.modeler.param.ModelerParams;
import co.cask.mmds.proto.BadRequestException;
import co.cask.mmds.proto.ConflictException;
import co.cask.mmds.proto.CreateModelRequest;
import co.cask.mmds.proto.ExperimentNotFoundException;
import co.cask.mmds.proto.ModelNotFoundException;
import co.cask.mmds.proto.SplitNotFoundException;
import co.cask.mmds.proto.TrainModelRequest;
import co.cask.mmds.stats.CategoricalHisto;
import co.cask.mmds.stats.NumericHisto;
import co.cask.mmds.stats.NumericStats;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manages experiments, splits, and models.
 */
public class ExperimentStore {
  private static final Set<Schema.Type> CATEGORICAL_TYPES = ImmutableSet.of(Schema.Type.BOOLEAN, Schema.Type.STRING);
  private static final Set<Schema.Type> NUMERIC_TYPES =
    ImmutableSet.of(Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE);
  private final ExperimentMetaTable experiments;
  private final DataSplitTable splits;
  private final ModelTable models;

  public ExperimentStore(ExperimentMetaTable experiments, DataSplitTable splits, ModelTable models) {
    this.experiments = experiments;
    this.splits = splits;
    this.models = models;
  }

  public ExperimentsMeta listExperiments(int offset, int limit) {
    return experiments.list(offset, limit);
  }

  public Experiment getExperiment(String experimentName) {
    Experiment experiment = experiments.get(experimentName);
    if (experiment == null) {
      throw new ExperimentNotFoundException(experimentName);
    }
    return experiment;
  }

  public ExperimentStats getExperimentStats(String experimentName) {
    Experiment experiment = getExperiment(experimentName);

    Map<String, ColumnStats> metricStats = new HashMap<>();
    CategoricalHisto algoHisto = new CategoricalHisto();
    CategoricalHisto statusHisto = new CategoricalHisto();

    List<ModelMeta> models = listModels(experimentName, 0, Integer.MAX_VALUE).getModels();
    if (models.isEmpty()) {
      return new ExperimentStats(experiment, metricStats, new ColumnStats(algoHisto), new ColumnStats(statusHisto));
    }

    Iterator<ModelMeta> modelIter = models.iterator();
    ModelMeta modelMeta = modelIter.next();
    algoHisto.update(modelMeta.getAlgorithm());
    statusHisto.update(modelMeta.getStatus() == null ? null : modelMeta.getStatus().toString());

    EvaluationMetrics metrics = modelMeta.getEvaluationMetrics();
    NumericStats rmse = new NumericStats(metrics.getRmse());
    NumericStats r2 = new NumericStats(metrics.getR2());
    NumericStats mae = new NumericStats(metrics.getMae());
    NumericStats evariance = new NumericStats(metrics.getEvariance());
    NumericStats precision = new NumericStats(metrics.getPrecision());
    NumericStats recall = new NumericStats(metrics.getRecall());
    NumericStats f1 = new NumericStats(metrics.getF1());

    while (modelIter.hasNext()) {
      modelMeta = modelIter.next();
      algoHisto.update(modelMeta.getAlgorithm());
      statusHisto.update(modelMeta.getStatus() == null ? null : modelMeta.getStatus().name());

      metrics = modelMeta.getEvaluationMetrics();
      rmse.update(metrics.getRmse());
      r2.update(metrics.getR2());
      mae.update(metrics.getMae());
      evariance.update(metrics.getEvariance());
      precision.update(metrics.getPrecision());
      recall.update(metrics.getRecall());
      f1.update(metrics.getF1());
    }

    modelIter = models.iterator();
    metrics = modelIter.next().getEvaluationMetrics();
    int numBins = Math.min(10, (int) statusHisto.getTotalCount());
    NumericHisto rmseHisto = null;
    if (rmse.getMin() != null) {
      rmseHisto = new NumericHisto(rmse.getMin(), rmse.getMax(), numBins, metrics.getRmse());
    }
    NumericHisto r2Histo = null;
    if (r2.getMin() != null) {
      r2Histo = new NumericHisto(r2.getMin(), r2.getMax(), numBins, metrics.getR2());
    }
    NumericHisto maeHisto = null;
    if (mae.getMin() != null) {
      maeHisto = new NumericHisto(mae.getMin(), mae.getMax(), numBins, metrics.getMae());
    }
    NumericHisto evarianceHisto = null;
    if (evariance.getMin() != null) {
      evarianceHisto = new NumericHisto(evariance.getMin(), evariance.getMax(), numBins, metrics.getEvariance());
    }
    NumericHisto precisionHisto = null;
    if (precision.getMin() != null) {
      precisionHisto = new NumericHisto(0, 1, 10, metrics.getPrecision());
    }
    NumericHisto recallHisto = null;
    if (recall.getMin() != null) {
      recallHisto = new NumericHisto(0, 1, 10, metrics.getRecall());
    }
    NumericHisto f1Histo = null;
    if (f1.getMin() != null) {
      f1Histo = new NumericHisto(0, 1, 10, metrics.getF1());
    }

    while (modelIter.hasNext()) {
      metrics = modelIter.next().getEvaluationMetrics();
      if (rmseHisto != null) {
        rmseHisto.update(metrics.getRmse());
      }
      if (r2Histo != null) {
        r2Histo.update(metrics.getR2());
      }
      if (maeHisto != null) {
        maeHisto.update(metrics.getMae());
      }
      if (evarianceHisto != null) {
        evarianceHisto.update(metrics.getEvariance());
      }
      if (precisionHisto != null) {
        precisionHisto.update(metrics.getPrecision());
      }
      if (recallHisto != null) {
        recallHisto.update(metrics.getRecall());
      }
      if (f1Histo != null) {
        f1Histo.update(metrics.getF1());
      }
    }

    if (rmseHisto != null) {
      metricStats.put("rmse", new ColumnStats(rmseHisto));
    }
    if (r2Histo != null) {
      metricStats.put("r2", new ColumnStats(r2Histo));
    }
    if (maeHisto != null) {
      metricStats.put("mae", new ColumnStats(maeHisto));
    }
    if (evarianceHisto != null) {
      metricStats.put("evariance", new ColumnStats(evarianceHisto));
    }
    if (precisionHisto != null) {
      metricStats.put("precision", new ColumnStats(precisionHisto));
    }
    if (recallHisto != null) {
      metricStats.put("recall", new ColumnStats(recallHisto));
    }
    if (f1Histo != null) {
      metricStats.put("f1", new ColumnStats(f1Histo));
    }

    return new ExperimentStats(experiment, metricStats, new ColumnStats(algoHisto), new ColumnStats(statusHisto));
  }

  public void putExperiment(Experiment experiment) {
    experiments.put(experiment);
  }

  public void deleteExperiment(String experimentName) {
    getExperiment(experimentName);
    models.delete(experimentName);
    splits.delete(experimentName);
    experiments.delete(experimentName);
  }

  public ModelsMeta listModels(String experimentName, int offset, int limit) {
    getExperiment(experimentName);
    return models.list(experimentName, offset, limit);
  }

  public ModelMeta getModel(ModelKey modelKey) {
    getExperiment(modelKey.getExperiment());
    ModelMeta modelMeta = models.get(modelKey);
    if (modelMeta == null) {
      throw new ModelNotFoundException(modelKey);
    }
    return modelMeta;
  }

  public ModelTrainerInfo trainModel(ModelKey key, TrainModelRequest trainRequest) {
    Experiment experiment = getExperiment(key.getExperiment());
    ModelMeta meta = getModel(key);
    ModelStatus currentStatus = meta.getStatus();

    if (currentStatus != ModelStatus.DATA_READY) {
      throw new ConflictException(String.format("Cannot train a model that is in the '%s' state.", currentStatus));
    }

    Modeler modeler = Modelers.getModeler(trainRequest.getAlgorithm());
    ModelerParams params = modeler.getParams(trainRequest.getHyperparameters());
    // update params with the modeler defaults.
    TrainModelRequest requestWithDefaults = new TrainModelRequest(trainRequest.getAlgorithm(),
                                                                  trainRequest.getPredictionsDataset(),
                                                                  params.toMap());

    models.setTrainingInfo(key, requestWithDefaults);

    SplitKey splitKey = new SplitKey(key.getExperiment(), meta.getSplit());
    DataSplitStats splitInfo = getSplit(splitKey);

    meta = ModelMeta.builder(meta)
      .setStatus(ModelStatus.TRAINING)
      .setAlgorithm(trainRequest.getAlgorithm())
      .setHyperParameters(trainRequest.getHyperparameters())
      .build();

    return new ModelTrainerInfo(experiment, splitInfo, key.getModel(), meta);
  }

  public void setModelSplit(ModelKey key, String splitId) {
    getExperiment(key.getExperiment());
    ModelMeta meta = getModel(key);
    ModelStatus currentStatus = meta.getStatus();

    if (currentStatus != ModelStatus.EMPTY && currentStatus != ModelStatus.SPLIT_FAILED &&
      currentStatus != ModelStatus.TRAINING_FAILED && currentStatus != ModelStatus.DATA_READY) {
      throw new ConflictException(String.format(
        "Cannot set a split for a model in the '%s' state. The model must be in the '%s', '%s', '%s', or '%s' state.",
        currentStatus, ModelStatus.EMPTY, ModelStatus.SPLIT_FAILED,
        ModelStatus.TRAINING_FAILED, ModelStatus.DATA_READY));
    }

    DataSplitStats splitInfo = getSplit(new SplitKey(key.getExperiment(), splitId));

    String currentSplit = meta.getSplit();
    if (currentSplit != null) {
      splits.unregisterModel(new SplitKey(key.getExperiment(), currentSplit), key.getModel());
    }

    models.setSplit(key, splitInfo);
    splits.registerModel(new SplitKey(key.getExperiment(), splitId), key.getModel());
  }

  public String addModel(String experimentName, CreateModelRequest createRequest) {
    Experiment experiment = getExperiment(experimentName);
    return models.add(experiment, createRequest, System.currentTimeMillis());
  }

  public void updateModelMetrics(ModelKey key, EvaluationMetrics evaluationMetrics,
                                 long trainedTime, List<String> features, Set<String> categoricalFeatures) {
    models.update(key, evaluationMetrics, trainedTime, features, categoricalFeatures);
  }

  public void deleteModel(ModelKey modelKey) {
    ModelMeta modelMeta = models.get(modelKey);
    if (modelMeta == null) {
      throw new ModelNotFoundException(modelKey);
    }
    models.delete(modelKey);
    if (modelMeta.getSplit() != null) {
      splits.unregisterModel(new SplitKey(modelKey.getExperiment(), modelMeta.getSplit()), modelKey.getModel());
    }
  }

  public void deployModel(ModelKey key) {
    ModelMeta modelMeta = getModel(key);
    if (modelMeta.getDeploytime() > 0) {
      // already deployed
      return;
    }
    models.setStatus(key, ModelStatus.DEPLOYED);
  }

  public void modelFailed(ModelKey key) {
    ModelMeta modelMeta = getModel(key);
    ModelStatus currentStatus = modelMeta.getStatus();
    if (currentStatus != ModelStatus.TRAINING) {
      // should never happen
      throw new IllegalStateException(String.format("Cannot transition model to '%s' from '%s'",
                                                    currentStatus, ModelStatus.TRAINING_FAILED));
    }
    models.setStatus(key, ModelStatus.TRAINING_FAILED);
  }

  public List<DataSplitStats> listSplits(String experimentName) {
    getExperiment(experimentName);
    return splits.list(experimentName);
  }

  public DataSplitInfo addSplit(String experimentName, DataSplit splitInfo) {
    Experiment experiment = getExperiment(experimentName);
    Schema.Type experimentOutcomeType = Schema.Type.valueOf(experiment.getOutcomeType().toUpperCase());

    Schema splitSchema = splitInfo.getSchema();
    Schema.Field outcomeField = splitSchema.getField(experiment.getOutcome());
    if (outcomeField == null) {
      throw new BadRequestException(
        String.format("Invalid split schema. The split must contain the experiment outcome '%s'.",
                      experiment.getOutcome()));
    }
    Schema splitOutcomeSchema = outcomeField.getSchema();
    if (splitOutcomeSchema.isNullable()) {
      splitOutcomeSchema = splitOutcomeSchema.getNonNullable();
    }
    Schema.Type splitOutcomeType = splitOutcomeSchema.getType();

    if (CATEGORICAL_TYPES.contains(experimentOutcomeType) && !CATEGORICAL_TYPES.contains(splitOutcomeType)) {
      throw new BadRequestException(
        String.format("Invalid split schema. Outcome field '%s' is of categorical type '%s' in the experiment , " +
                        "but is of non-categorical type '%s' in the split.",
                      experiment.getOutcome(), experimentOutcomeType, splitOutcomeType));
    }
    if (NUMERIC_TYPES.contains(experimentOutcomeType) && !NUMERIC_TYPES.contains(splitOutcomeType)) {
      throw new BadRequestException(
        String.format("Invalid split schema. Outcome field '%s' is of numeric type '%s' in the experiment, " +
                        "but is of non-numeric type '%s' in the split.",
                      experiment.getOutcome(), experimentOutcomeType, splitOutcomeType));
    }

    String splitId = splits.addSplit(experimentName, splitInfo);
    return new DataSplitInfo(splitId, experiment, splitInfo);
  }

  public DataSplitStats getSplit(SplitKey key) {
    getExperiment(key.getExperiment());

    DataSplitStats stats = splits.get(key);
    if (stats == null) {
      throw new SplitNotFoundException(key);
    }
    return stats;
  }

  public void finishSplit(SplitKey splitKey, String trainingPath, String testPath, List<ColumnSplitStats> stats) {
    splits.updateStats(splitKey, trainingPath, testPath, stats);
    DataSplitStats splitStats = getSplit(splitKey);
    for (String modelId : splitStats.getModels()) {
      models.setStatus(new ModelKey(splitKey.getExperiment(), modelId), ModelStatus.DATA_READY);
    }
  }

  public void deleteSplit(SplitKey key) {
    DataSplitStats stats = getSplit(key);
    if (!stats.getModels().isEmpty()) {
      throw new ConflictException(String.format("Cannot delete split '%s' since it is used by model(s) '%s'.",
                                                key.getSplit(), Joiner.on(',').join(stats.getModels())));
    }
    splits.delete(key);
  }
}
