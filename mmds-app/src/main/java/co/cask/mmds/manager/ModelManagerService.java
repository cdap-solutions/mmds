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

package co.cask.mmds.manager;

import co.cask.cdap.api.spark.AbstractExtendedSpark;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.api.spark.JavaSparkMain;
import co.cask.mmds.Constants;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages machine learning experiments and models
 */
public class ModelManagerService extends AbstractExtendedSpark implements JavaSparkMain {
  public static final String NAME = "ModelManagerService";
  private final String modelMetaDataset;
  private final String modelComponentsDataset;
  private final String experimentMetaDataset;
  private final String splitsDataset;

  public ModelManagerService() {
    // required for creation through Class.newInstance()
    this(Constants.Dataset.MODEL_META, Constants.Dataset.MODEL_COMPONENTS,
         Constants.Dataset.EXPERIMENTS_META, Constants.Dataset.SPLITS);
  }

  ModelManagerService(String modelMetaDataset, String modelComponentsDataset,
                      String experimentMetaDataset, String splitsDataset) {
    this.modelMetaDataset = modelMetaDataset;
    this.modelComponentsDataset = modelComponentsDataset;
    this.experimentMetaDataset = experimentMetaDataset;
    this.splitsDataset = splitsDataset;
  }

  @Override
  protected void configure() {
    setName(NAME);
    setDescription("Manages Machine Learning Experiments and Models");
    Map<String, String> properties = new HashMap<>();
    properties.put("modelMetaDataset", modelMetaDataset);
    properties.put("modelComponentsDataset", modelComponentsDataset);
    properties.put("experimentMetaDataset", experimentMetaDataset);
    properties.put("splitsDataset", splitsDataset);
    setProperties(properties);
    addHandlers(new ModelManagerServiceHandler());
    setMainClass(ModelManagerService.class);
  }

  @Override
  public void run(JavaSparkExecutionContext javaSparkExecutionContext) throws Exception {
    SparkSession.builder().appName("Model Management Service").getOrCreate();
  }
}
