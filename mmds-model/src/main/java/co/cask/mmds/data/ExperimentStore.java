package co.cask.mmds.data;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.mmds.proto.BadRequestException;
import co.cask.mmds.proto.ConflictException;
import co.cask.mmds.proto.ExperimentNotFoundException;
import co.cask.mmds.proto.ModelNotFoundException;
import co.cask.mmds.proto.SplitNotFoundException;
import co.cask.mmds.stats.CategoricalHisto;
import co.cask.mmds.stats.NumericHisto;
import co.cask.mmds.stats.NumericStats;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manages experiments, splits, and models.
 */
public class ExperimentStore {
  private static final Logger LOG = LoggerFactory.getLogger(ExperimentStore.class);
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

  public List<Experiment> listExperiments() {
    return experiments.list();
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

    Map<String, ColumnStats> stats = new HashMap<>();
    CategoricalHisto algoHisto = new CategoricalHisto();
    CategoricalHisto statusHisto = new CategoricalHisto();

    List<ModelMeta> models = listModels(experimentName);
    if (models.isEmpty()) {
      return new ExperimentStats(experiment, stats, new ColumnStats(algoHisto), new ColumnStats(statusHisto));
    }

    Iterator<ModelMeta> modelIter = models.iterator();
    ModelMeta modelMeta = modelIter.next();
    algoHisto.update(modelMeta.getAlgorithm());
    statusHisto.update(modelMeta.getStatus() == null ? null : modelMeta.getStatus().name());

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
      stats.put("rmse", new ColumnStats(rmseHisto));
    }
    if (r2Histo != null) {
      stats.put("r2", new ColumnStats(r2Histo));
    }
    if (maeHisto != null) {
      stats.put("mae", new ColumnStats(maeHisto));
    }
    if (evarianceHisto != null) {
      stats.put("evariance", new ColumnStats(evarianceHisto));
    }
    if (precisionHisto != null) {
      stats.put("precision", new ColumnStats(precisionHisto));
    }
    if (recallHisto != null) {
      stats.put("recall", new ColumnStats(recallHisto));
    }
    if (f1Histo != null) {
      stats.put("f1", new ColumnStats(f1Histo));
    }

    return new ExperimentStats(experiment, stats, new ColumnStats(algoHisto), new ColumnStats(statusHisto));
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

  public List<ModelMeta> listModels(String experimentName) {
    getExperiment(experimentName);
    return models.list(experimentName);
  }

  public ModelMeta getModel(ModelKey modelKey) {
    getExperiment(modelKey.getExperiment());
    ModelMeta modelMeta = models.get(modelKey);
    if (modelMeta == null) {
      throw new ModelNotFoundException(modelKey);
    }
    return modelMeta;
  }

  public ModelTrainerInfo addModel(String experimentName, Model modelInfo) {
    Experiment experiment = getExperiment(experimentName);
    SplitKey splitKey = new SplitKey(experimentName, modelInfo.getSplit());
    DataSplitStats splitInfo = splits.get(splitKey);
    if (splitInfo == null) {
      throw new SplitNotFoundException(splitKey);
    }
    if (splitInfo.getTrainingPath() == null) {
      throw new ConflictException("Data split is not ready. " +
                                    "Please try again after the split has finished successfully.");
    }
    String modelId = models.add(experimentName, experiment.getOutcome(), modelInfo, System.currentTimeMillis());
    splits.registerModel(splitKey, modelId);
    return new ModelTrainerInfo(experiment, splitInfo, modelId, modelInfo);
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
    splits.unregisterModel(new SplitKey(modelKey.getExperiment(), modelMeta.getSplit()), modelKey.getModel());
  }

  public void deployModel(ModelKey key) {
    ModelMeta modelMeta = getModel(key);
    if (modelMeta.getDeploytime() > 0) {
      // already deployed
      return;
    }
    models.setStatus(key, ModelStatus.DEPLOYED);
  }

  public void setModelStatus(ModelKey key, ModelStatus status) {
    ModelMeta modelMeta = getModel(key);
    ModelStatus currentStatus = modelMeta.getStatus();
    switch (status) {
      case TRAINING:
        if (currentStatus != ModelStatus.WAITING) {
          throw new ConflictException("Cannot transition model state from " + currentStatus + " to " + status);
        }
        break;
      case TRAINED:
        if (currentStatus != ModelStatus.TRAINING) {
          throw new ConflictException("Cannot transition model state from " + currentStatus + " to " + status);
        }
        break;
      case DEPLOYED:
        if (currentStatus != ModelStatus.TRAINED) {
          throw new ConflictException("Cannot transition model state from " + currentStatus + " to " + status);
        }
        break;
    }
    models.setStatus(key, status);
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

  public void updateSplitStats(SplitKey splitKey, String trainingPath, String testPath,
                               Map<String, ColumnStats> trainingStats, Map<String, ColumnStats> testStats) {
    splits.updateStats(splitKey, trainingPath, testPath, trainingStats, testStats);
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
