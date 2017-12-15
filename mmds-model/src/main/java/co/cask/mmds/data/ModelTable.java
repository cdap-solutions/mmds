package co.cask.mmds.data;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scan;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * A thin layer on top of the underlying Table that stores the model meta data. Handles scanning, deletion,
 * serialization, deserialization, etc. This is not a custom dataset because custom datasets cannot currently
 * be used in plugins.
 */
public class ModelTable {
  private static final Gson GSON = new Gson();
  private static Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static Type LIST_TYPE = new TypeToken<List<String>>() { }.getType();
  private static Type SET_TYPE = new TypeToken<Set<String>>() { }.getType();
  private static final String SEPARATOR = "/";

  // model columns
  private static final String EXPERIMENT_COL = "experiment";
  private static final String ID_COL = "id";
  private static final String NAME_COL = "name";
  private static final String DESC_COL = "description";
  private static final String ALGO_COL = "algorithm";
  private static final String SPLIT_COL = "split";
  private static final String OUTCOME_COL = "outcome";
  private static final String HYPER_PARAMS_COL = "hyperparameters";
  private static final String FEATURES_COL = "features";
  private static final String CATEGORICAL_FEATURES_COL = "catfeatures";
  private static final String CREATE_TIME_COL = "createtime";
  private static final String TRAIN_TIME_COL = "trainedtime";
  private static final String DEPLOY_TIME_COL = "deploytime";
  private static final String STATUS_COL = "status";
  // evaluation metric columns
  private static final String PRECISION_COL = "precision";
  private static final String RECALL_COL = "recall";
  private static final String F1_COL = "f1";
  private static final String RMSE_COL = "rmse";
  private static final String R2_COL = "r2";
  private static final String EVARIANCE_COL = "evariance";
  private static final String MAE_COL = "mae";
  
  private final Table table;

  public ModelTable(Table table) {
    this.table = table;
  }

  /**
   * List all models in the specified experiment. Never returns null. If there are no models, returns an empty list.
   *
   * @param experiment the experiment name
   * @return all models in the experiment
   */
  public List<ModelMeta> list(String experiment) {
    byte[] startKey = Bytes.toBytes(experiment + SEPARATOR);
    Scan scan = new Scan(startKey, Bytes.stopKeyForPrefix(startKey));

    List<ModelMeta> models = new ArrayList<>();
    try (Scanner scanner = table.scan(scan)) {
      Row row;
      while ((row = scanner.next()) != null) {
        models.add(fromRow(row));
      }
    }
    return models;
  }

  /**
   * Get metadata for the specified model.
   *
   * @param key the model key
   * @return metadata for the specified model
   */
  @Nullable
  public ModelMeta get(ModelKey key) {
    Row row = table.get(getKey(key));
    return row.isEmpty() ? null : fromRow(row);
  }

  /**
   * Set status for the specified model.
   *
   * @param key the model key
   * @param status status of the model
   */
  public void setStatus(ModelKey key, ModelStatus status) {
    Put put = new Put(getKey(key)).add(STATUS_COL, status.name());
    if (status == ModelStatus.DEPLOYED) {
      put.add(DEPLOY_TIME_COL, System.currentTimeMillis());
    } else if (status == ModelStatus.TRAINED) {
      put.add(TRAIN_TIME_COL, System.currentTimeMillis());
    }
    table.put(put);
  }

  /**
   * Delete the specified model.
   *
   * @param key the model key
   */
  public void delete(ModelKey key) {
    table.delete(getKey(key));
  }

  /**
   * Delete all models in the specified experiment. Returns the number of models deleted.
   *
   * @param experiment the experiment to delete all models in
   * @return the number of models deleted
   */
  public int delete(String experiment) {
    return delete(experiment, Integer.MAX_VALUE);
  }

  /**
   * Delete up to limit models in the specified experiment. Returns the number of models deleted.
   * This can be used to delete models in chunks, which can be useful if there is concern that not all models
   * can deleted in a single transaction.
   *
   * @param experiment the experiment to delete all models in
   * @param limit maximum number of models to delete
   * @return the number of models deleted
   */
  public int delete(String experiment, int limit) {
    byte[] startKey = Bytes.toBytes(experiment + SEPARATOR);
    Scan scan = new Scan(startKey, Bytes.stopKeyForPrefix(startKey));

    List<byte[]> keys = new ArrayList<>();
    int numKeys = 0;
    try (Scanner scanner = table.scan(scan)) {
      Row row;
      while ((row = scanner.next()) != null) {
        keys.add(row.getRow());
        numKeys++;
        if (numKeys >= limit) {
          break;
        }
      }
    }

    for (byte[] key : keys) {
      table.delete(key);
    }
    return numKeys;
  }

  /**
   * Add a new model to the specified experiment.
   *
   * @param experiment the experiment to add the model to
   * @param outcome the outcome the model will predict
   * @param model the model information
   * @return the id for the newly added model
   */
  public String add(String experiment, String outcome, Model model, long createTs) {
    String id = UUID.randomUUID().toString().replaceAll("-", "");
    Put put = new Put(getKey(experiment, id))
      .add(EXPERIMENT_COL, experiment)
      .add(ID_COL, id)
      .add(NAME_COL, model.getName())
      .add(DESC_COL, model.getDescription())
      .add(OUTCOME_COL, outcome)
      .add(ALGO_COL, model.getAlgorithm())
      .add(SPLIT_COL, model.getSplit())
      .add(HYPER_PARAMS_COL, GSON.toJson(model.getHyperparameters()))
      .add(CREATE_TIME_COL, createTs)
      .add(STATUS_COL, ModelStatus.WAITING.name())
      .add(TRAIN_TIME_COL, -1L)
      .add(DEPLOY_TIME_COL, -1L);
    table.put(put);
    return id;
  }

  /**
   * Update the model metadata after it has been trained.
   *
   * @param key the model key
   * @param evaluationMetrics the model evaluation metrics
   */
  public void update(ModelKey key, EvaluationMetrics evaluationMetrics,
                     long trainedTime, List<String> features, Set<String> categoricalFeatures) {
    Put put = new Put(getKey(key));
    if (evaluationMetrics.getPrecision() != null) {
      put.add(PRECISION_COL, evaluationMetrics.getPrecision());
    }
    if (evaluationMetrics.getRecall() != null) {
      put.add(RECALL_COL, evaluationMetrics.getRecall());
    }
    if (evaluationMetrics.getF1() != null) {
      put.add(F1_COL, evaluationMetrics.getF1());
    }
    if (evaluationMetrics.getRmse() != null) {
      put.add(RMSE_COL, evaluationMetrics.getRmse());
    }
    if (evaluationMetrics.getR2() != null) {
      put.add(R2_COL, evaluationMetrics.getR2());
    }
    if (evaluationMetrics.getEvariance() != null) {
      put.add(EVARIANCE_COL, evaluationMetrics.getEvariance());
    }
    if (evaluationMetrics.getMae() != null) {
      put.add(MAE_COL, evaluationMetrics.getMae());
    }
    put.add(STATUS_COL, ModelStatus.TRAINED.name());
    put.add(TRAIN_TIME_COL, trainedTime);
    put.add(FEATURES_COL, GSON.toJson(features));
    put.add(CATEGORICAL_FEATURES_COL, GSON.toJson(categoricalFeatures));
    table.put(put);
  }

  private ModelMeta fromRow(Row row) {
    String keyStr = Bytes.toString(row.getRow());
    int idx = keyStr.indexOf(SEPARATOR);
    String modelId = keyStr.substring(idx + 1);
    Map<String, String> hyperParameters = GSON.fromJson(row.getString(HYPER_PARAMS_COL), MAP_TYPE);
    List<String> features = GSON.fromJson(row.getString(FEATURES_COL), LIST_TYPE);
    features = features == null ? new ArrayList<>() : features;
    Set<String> categoricalFeatures = GSON.fromJson(row.getString(CATEGORICAL_FEATURES_COL), SET_TYPE);
    categoricalFeatures = categoricalFeatures == null ? new HashSet<>() : categoricalFeatures;
    String description = row.getString(DESC_COL);
    description = description == null ? "" : description;
    String statusStr = row.getString(STATUS_COL);
    ModelStatus status = statusStr == null ? null : ModelStatus.valueOf(statusStr);

    EvaluationMetrics evaluationMetrics = new EvaluationMetrics(
      row.getDouble(PRECISION_COL), row.getDouble(RECALL_COL), row.getDouble(F1_COL),
      row.getDouble(RMSE_COL), row.getDouble(R2_COL), row.getDouble(EVARIANCE_COL), row.getDouble(MAE_COL));
    return ModelMeta.builder(modelId)
      .setName(row.getString(NAME_COL))
      .setDescription(description)
      .setOutcome(row.getString(OUTCOME_COL))
      .setAlgorithm(row.getString(ALGO_COL))
      .setSplit(row.getString(SPLIT_COL))
      .setHyperParameters(hyperParameters)
      .setFeatures(features)
      .setStatus(status)
      .setCategoricalFeatures(categoricalFeatures)
      .setCreateTime(row.getLong(CREATE_TIME_COL, -1))
      .setTrainedTime(row.getLong(TRAIN_TIME_COL, -1))
      .setDeployTime(row.getLong(DEPLOY_TIME_COL, -1))
      .setEvaluationMetrics(evaluationMetrics)
      .build();
  }

  private byte[] getKey(ModelKey key) {
    return getKey(key.getExperiment(), key.getModel());
  }

  private byte[] getKey(String experiment, String model) {
    return Bytes.toBytes(experiment + SEPARATOR + model);
  }
}
