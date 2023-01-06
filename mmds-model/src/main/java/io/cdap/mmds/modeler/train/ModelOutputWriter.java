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

package io.cdap.mmds.modeler.train;

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.PartitionDetail;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.dataset.lib.PartitionOutput;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetProperties;
import io.cdap.cdap.api.dataset.lib.Partitioning;
import io.cdap.mmds.Constants;
import io.cdap.mmds.api.AlgorithmType;
import io.cdap.mmds.data.ModelKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.spark.sql.SaveMode;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes Model components.
 */
public class ModelOutputWriter {
  private static final Logger LOG = LoggerFactory.getLogger(ModelOutputWriter.class);
  private final Admin admin;
  private final Transactional transactional;
  private final Location baseLocation;
  private final boolean overwrite;

  public ModelOutputWriter(Admin admin, Transactional transactional, Location baseLocation, boolean overwrite) {
    this.admin = admin;
    this.transactional = transactional;
    this.baseLocation = baseLocation;
    this.overwrite = overwrite;
  }

  public void save(ModelKey modelKey, ModelOutput modelOutput, @Nullable String predictionsDataset) throws Exception {
    // save target indices (target is categorical)
    if (modelOutput.getTargetIndexModel() != null) {
      LOG.info("Saving outcome indices...");
      String path = getPath(modelKey, Constants.Component.TARGET_INDICES);
      modelOutput.getTargetIndexModel().save(path);
      LOG.info("Outcome indices successfully saved.");
    }

    // save feature gen pipeline
    LOG.info("Saving feature generation pipeline...");
    String featureGenPath = getPath(modelKey, Constants.Component.FEATUREGEN);
    modelOutput.getFeatureGenModel().write().overwrite().save(featureGenPath);
    LOG.info("Feature generation pipeline successfully saved.");

    // save the model
    LOG.info("Saving trained model...");
    String modelPath = getPath(modelKey, Constants.Component.MODEL);
    modelOutput.getModel().write().overwrite().save(modelPath);
    LOG.info("Model successfully saved.");

    if (predictionsDataset != null) {
      if (!admin.datasetExists(predictionsDataset)) {
        List<Schema.Field> predictionFields = new ArrayList<>();
        Schema.Type predictionType = modelOutput.getAlgorithmType() == AlgorithmType.REGRESSION ?
          Schema.Type.DOUBLE : Schema.Type.STRING;
        predictionFields.add(Schema.Field.of("prediction", Schema.of(predictionType)));
        predictionFields.addAll(modelOutput.getSchema().getFields());
        Schema predictionSchema =
          Schema.recordOf(modelOutput.getSchema().getRecordName() + ".prediction", predictionFields);
        DatasetProperties datasetProperties = PartitionedFileSetProperties.builder()
          .setPartitioning(Partitioning.builder().addStringField("experiment").addStringField("model").build())
          .build();

        admin.createDataset(predictionsDataset, PartitionedFileSet.class.getName(), datasetProperties);
      }

      PartitionKey predictionsPartitionKey = PartitionKey.builder()
        .addStringField("model", modelKey.getModel())
        .addStringField("experiment", modelKey.getExperiment())
        .build();
      AtomicReference<String> path = new AtomicReference<>();
      transactional.execute(datasetContext -> {
        PartitionedFileSet predictionsFileset = datasetContext.getDataset(predictionsDataset);
        PartitionDetail partitionDetail = predictionsFileset.getPartition(predictionsPartitionKey);
        if (partitionDetail == null) {
          PartitionOutput partitionOutput = predictionsFileset.getPartitionOutput(predictionsPartitionKey);
          path.set(partitionOutput.getLocation().toURI().getPath());
          partitionOutput.addPartition();
        } else {
          path.set(partitionDetail.getLocation().toURI().getPath());
        }
      });

      // save predictions
      modelOutput.getPredictions().write().format("csv").mode(SaveMode.Overwrite).save(path.get());
      LOG.info("Predictions on training data successfully saved.");
    }
  }

  public void deleteComponents(ModelKey modelKey) throws IOException {
    deleteComponent(modelKey, Constants.Component.TARGET_INDICES);
    deleteComponent(modelKey, Constants.Component.FEATUREGEN);
    deleteComponent(modelKey, Constants.Component.MODEL);
  }

  private String getPath(ModelKey modelKey, String component) throws IOException {
    Location location = baseLocation.append(modelKey.getExperiment()).append(modelKey.getModel()).append(component);
    if (location.exists()) {
      if (overwrite) {
        location.delete();
      } else {
        throw new IllegalArgumentException(location + " already exists.");
      }
    }
    return location.toURI().getPath();
  }

  private void deleteComponent(ModelKey modelKey, String component) throws IOException {
    Location location = baseLocation.append(modelKey.getExperiment()).append(modelKey.getModel()).append(component);
    if (location.exists()) {
      location.delete();
    }
  }
}
