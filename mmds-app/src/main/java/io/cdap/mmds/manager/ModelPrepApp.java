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

import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.IndexedTable;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.table.Table;
import com.google.common.annotations.VisibleForTesting;
import io.cdap.mmds.Constants;
import io.cdap.mmds.data.DataSplitTable;
import io.cdap.mmds.data.ExperimentMetaTable;
import io.cdap.mmds.data.ModelTable;


/**
 * Manage all the experiments and models.
 */
public class ModelPrepApp extends AbstractApplication<ModelPrepApp.Conf> {
  private static final String NAME = "ModelManagementApp";
  private static final String DESCRIPTION = "Model Management App";

  @Override
  public void configure() {
    setName(NAME);
    setDescription(DESCRIPTION);

    Conf conf = getConfig();
    String modelMetaName = conf.getModelMetaDataset();
    String modelComponentsName = conf.getModelComponentDataset();
    String experimentMetaName = conf.getExperimentMetaDataset();
    String splitsName = conf.getSplitsDataset();
    createDataset(experimentMetaName, IndexedTable.class, ExperimentMetaTable.DATASET_PROPERTIES);
    createDataset(modelMetaName, IndexedTable.class, ModelTable.DATASET_PROPERTIES);
    createDataset(modelComponentsName, FileSet.class, DatasetProperties.EMPTY);
    createDataset(splitsName, PartitionedFileSet.class, DataSplitTable.DATASET_PROPERTIES);

    addSpark(new ModelManagerService(modelMetaName, modelComponentsName, experimentMetaName, splitsName));
  }

  /**
   * Conf that allows overriding default dataset names.
   */
  public static class Conf extends Config {
    private String modelMetaDataset;
    private String modelComponentsDataset;
    private String experimentMetaDataset;
    private String splitsDataset;

    public Conf() {
      this(Constants.Dataset.MODEL_META, Constants.Dataset.MODEL_COMPONENTS,
           Constants.Dataset.EXPERIMENTS_META, Constants.Dataset.SPLITS);
    }

    @VisibleForTesting
    public Conf(String modelMetaDataset, String modelComponentsDataset,
                String experimentMetaDataset, String splitsDataset) {
      this.modelMetaDataset = modelMetaDataset;
      this.modelComponentsDataset = modelComponentsDataset;
      this.experimentMetaDataset = experimentMetaDataset;
      this.splitsDataset = splitsDataset;
    }

    public String getModelMetaDataset() {
      return modelMetaDataset;
    }

    public String getModelComponentDataset() {
      return modelComponentsDataset;
    }

    public String getExperimentMetaDataset() {
      return experimentMetaDataset;
    }

    public String getSplitsDataset() {
      return splitsDataset;
    }
  }
}
