package co.cask.mmds.data;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionMetadata;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Lists, gets, adds, and deletes data splits in an experiment.
 */
public class DataSplitTable {
  private static final Type SET_TYPE = new TypeToken<Set<String>>() { }.getType();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Type LIST_TYPE = new TypeToken<List<String>>() { }.getType();
  private static final Type STATS_TYPE = new TypeToken<Map<String, ColumnStats>>() { }.getType();
  private static final Gson GSON = new Gson();
  private static final String EXPERIMENT = "exp";
  private static final String SPLIT = "split";
  private static final String MODELS = "models";
  private static final String DESCRIPTION = "desc";
  private static final String TYPE = "type";
  private static final String PARAMS = "params";
  private static final String DIRECTIVES = "directives";
  private static final String SCHEMA = "schema";
  private static final String TEST_PATH = "test.path";
  private static final String TRAIN_PATH = "train.path";
  private static final String TEST_STATS = "test.stats";
  private static final String TRAIN_STATS = "train.stats";
  public static final DatasetProperties DATASET_PROPERTIES = PartitionedFileSetProperties.builder()
    .setPartitioning(Partitioning.builder().addStringField(EXPERIMENT).addStringField(SPLIT).build())
    .setEnableExploreOnCreate(false)
    .setDescription("Contains data splits used to train models")
    .build();
  private final PartitionedFileSet splits;

  public DataSplitTable(PartitionedFileSet splits) {
    this.splits = splits;
  }

  public String addSplit(String experiment, DataSplit dataSplit) {
    String id = UUID.randomUUID().toString().replaceAll("-", "");
    PartitionKey key = PartitionKey.builder().addStringField(EXPERIMENT, experiment).addStringField(SPLIT, id).build();
    PartitionOutput partitionOutput = splits.getPartitionOutput(key);

    Map<String, String> meta = new HashMap<>();
    meta.put(DESCRIPTION, dataSplit.getDescription());
    meta.put(TYPE, dataSplit.getType());
    meta.put(PARAMS, GSON.toJson(dataSplit.getParams()));
    meta.put(DIRECTIVES, GSON.toJson(dataSplit.getDirectives()));
    meta.put(SCHEMA, dataSplit.getSchema().toString());

    partitionOutput.setMetadata(meta);
    partitionOutput.addPartition();
    return id;
  }

  public List<DataSplitStats> list(String experiment) {
    List<DataSplitStats> output = new ArrayList<>();
    PartitionFilter filter = PartitionFilter.builder()
      .addValueCondition(EXPERIMENT, experiment)
      .build();
    for (PartitionDetail partitionDetail : splits.getPartitions(filter)) {
      output.add(toSplitStats(partitionDetail, true));
    }
    return output;
  }

  @Nullable
  public DataSplitStats get(SplitKey key) {
    PartitionDetail partitionDetail = splits.getPartition(getKey(key));
    if (partitionDetail == null) {
      return null;
    }
    return toSplitStats(partitionDetail, false);
  }

  @Nullable
  public Location getLocation(SplitKey key) {
    PartitionDetail partitionDetail = splits.getPartition(getKey(key));
    if (partitionDetail == null) {
      return null;
    }
    return partitionDetail.getLocation();
  }

  /**
   * Delete the specified split
   *
   * @param splitKey the split to delete
   */
  public void delete(SplitKey splitKey) {
    splits.dropPartition(getKey(splitKey));
  }

  /**
   * Delete all splits in an experiment
   *
   * @param experiment the experiment to delete splits in
   */
  public void delete(String experiment) {
    PartitionFilter filter = PartitionFilter.builder()
      .addValueCondition(EXPERIMENT, experiment)
      .build();
    for (PartitionDetail partitionDetail : splits.getPartitions(filter)) {
      splits.dropPartition(partitionDetail.getPartitionKey());
    }
  }

  /**
   * Update information about the split once the actual splitting has been carried out.
   *
   * @param splitKey the split to update
   * @param trainingPath path to training data
   * @param testPath path to test data
   * @param trainingStats stats about the training data
   * @param testStats stats about the test data
   */
  public void updateStats(SplitKey splitKey, String trainingPath, String testPath,
                          Map<String, ColumnStats> trainingStats, Map<String, ColumnStats> testStats) {
    PartitionKey key = getKey(splitKey);
    Map<String, String> updates = new HashMap<>();
    updates.put(TRAIN_PATH, trainingPath);
    updates.put(TEST_PATH, testPath);
    updates.put(TRAIN_STATS, GSON.toJson(trainingStats));
    updates.put(TEST_STATS, GSON.toJson(testStats));
    splits.setMetadata(key, updates);
  }

  /**
   * Register that a model is no longer using the specified split
   *
   * @param splitKey the split key
   * @param model the model id
   */
  public void unregisterModel(SplitKey splitKey, String model) {
    PartitionKey key = getKey(splitKey);
    PartitionDetail partitionDetail = splits.getPartition(key);
    if (partitionDetail == null) {
      return;
    }

    Map<String, String> updates = new HashMap<>();
    String modelsStr = partitionDetail.getMetadata().get(MODELS);
    Set<String> models = new HashSet<>();
    if (modelsStr != null) {
      models = GSON.fromJson(modelsStr, SET_TYPE);
    }
    models.remove(model);
    updates.put(MODELS, GSON.toJson(models));

    splits.setMetadata(key, updates);
  }

  /**
   * Register that a model is using the specified split
   *
   * @param splitKey the split key
   * @param model the model id
   */
  public void registerModel(SplitKey splitKey, String model) {
    PartitionKey key = getKey(splitKey);
    PartitionDetail partitionDetail = splits.getPartition(key);

    Map<String, String> updates = new HashMap<>();
    String modelsStr = partitionDetail.getMetadata().get(MODELS);
    Set<String> models = new HashSet<>();
    if (modelsStr != null) {
      models = GSON.fromJson(modelsStr, SET_TYPE);
    }
    models.add(model);
    updates.put(MODELS, GSON.toJson(models));

    splits.setMetadata(key, updates);
  }

  private PartitionKey getKey(SplitKey key) {
    return PartitionKey.builder()
      .addStringField(EXPERIMENT, key.getExperiment())
      .addStringField(SPLIT, key.getSplit())
      .build();
  }

  private DataSplitStats toSplitStats(PartitionDetail partitionDetail, boolean excludeStats) {
    String id = (String) partitionDetail.getPartitionKey().getField(SPLIT);
    PartitionMetadata meta = partitionDetail.getMetadata();
    Map<String, String> params = GSON.fromJson(meta.get(PARAMS), MAP_TYPE);
    List<String> directives = GSON.fromJson(meta.get(DIRECTIVES), LIST_TYPE);
    Schema schema;
    try {
      schema = Schema.parseJson(meta.get(SCHEMA));
    } catch (IOException e) {
      // should never happen
      throw new IllegalStateException("Unable to parse split schema. This is likely due to data corruption. " +
                                        "You will likely have to delete the split.");
    }

    DataSplitStats.Builder builder = DataSplitStats.builder(id)
      .setDescription(meta.get(DESCRIPTION))
      .setType(meta.get(TYPE))
      .setParams(params)
      .setDirectives(directives)
      .setSchema(schema)
      .setTrainingPath(meta.get(TRAIN_PATH))
      .setTestPath(meta.get(TEST_PATH));

    if (!excludeStats) {
      String trainStatsStr = meta.get(TRAIN_STATS);
      String testStatsStr = meta.get(TEST_STATS);
      Map<String, ColumnStats> trainStats = new HashMap<>();
      if (trainStatsStr != null) {
        trainStats = GSON.fromJson(trainStatsStr, STATS_TYPE);
      }
      Map<String, ColumnStats> testStats = new HashMap<>();
      if (testStatsStr != null) {
        testStats = GSON.fromJson(testStatsStr, STATS_TYPE);
      }
      builder.setTrainingStats(trainStats).setTestStats(testStats);
    }
    return builder.build();
  }
}
