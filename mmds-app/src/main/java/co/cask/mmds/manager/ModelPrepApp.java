/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.mmds.manager;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.mmds.Constants;
import co.cask.mmds.data.DataSplitTable;
import co.cask.mmds.data.ExperimentMetaTable;

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
    createDataset(modelMetaName, Table.class, DatasetProperties.EMPTY);
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

    public String getModelMetaDataset() {
      return modelMetaDataset == null ? Constants.Dataset.MODEL_META : modelMetaDataset;
    }

    public String getModelComponentDataset() {
      return modelComponentsDataset == null ? Constants.Dataset.MODEL_COMPONENTS : modelComponentsDataset;
    }

    public String getExperimentMetaDataset() {
      return experimentMetaDataset == null ? Constants.Dataset.EXPERIMENTS_META : experimentMetaDataset;
    }

    public String getSplitsDataset() {
      return splitsDataset == null ? Constants.Dataset.SPLITS : splitsDataset;
    }
  }
}
