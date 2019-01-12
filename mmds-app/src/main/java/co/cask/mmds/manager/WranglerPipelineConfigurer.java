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


import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.module.DatasetModule;
import co.cask.cdap.api.plugin.PluginConfigurer;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.api.plugin.PluginSelector;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageConfigurer;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implements configurer for WranglerTransform. Required to register UDDs.
 */
// TODO: remove usage of Hydrator classes. Requires wrangler work
public class WranglerPipelineConfigurer implements PipelineConfigurer {
  private static final Schema TEXT_SCHEMA =
    Schema.recordOf("textRecord", Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
  private final PluginConfigurer pluginConfigurer;

  public WranglerPipelineConfigurer(PluginConfigurer pluginConfigurer) {
    this.pluginConfigurer = pluginConfigurer;
  }

  @Override
  public StageConfigurer getStageConfigurer() {
    return new StageConfigurer() {
      @Nullable
      @Override
      public Schema getInputSchema() {
        return TEXT_SCHEMA;
      }

      @Override
      public void setOutputSchema(@Nullable Schema schema) {
        // no-op
      }

      @Override
      public void setErrorSchema(@Nullable Schema schema) {
        // no-op
      }
    };
  }

  @Override
  public Engine getEngine() {
    return Engine.SPARK;
  }

  @Override
  public void setPipelineProperties(Map<String, String> map) {
    // no-op
  }

  @Override
  public void addDatasetModule(String s, Class<? extends DatasetModule> aClass) {
    throw new UnsupportedOperationException("Cannot add dataset modules in MMDS.");
  }

  @Override
  public void addDatasetType(Class<? extends Dataset> aClass) {
    throw new UnsupportedOperationException("Cannot add dataset types in MMDS.");

  }

  @Override
  public void createDataset(String s, String s1, DatasetProperties datasetProperties) {
    throw new UnsupportedOperationException("Cannot create datasets in MMDS.");
  }

  @Override
  public void createDataset(String s, String s1) {
    throw new UnsupportedOperationException("Cannot create datasets in MMDS.");
  }

  @Override
  public void createDataset(String s, Class<? extends Dataset> aClass, DatasetProperties datasetProperties) {
    throw new UnsupportedOperationException("Cannot create datasets in MMDS.");
  }

  @Override
  public void createDataset(String s, Class<? extends Dataset> aClass) {
    throw new UnsupportedOperationException("Cannot create datasets in MMDS.");
  }

  @Nullable
  @Override
  public <T> T usePlugin(String pluginType, String pluginName, String pluginId,
                         PluginProperties pluginProperties, PluginSelector pluginSelector) {
    return pluginConfigurer.usePlugin(pluginType, pluginName, pluginId, pluginProperties, pluginSelector);
  }

  @Nullable
  @Override
  public <T> Class<T> usePluginClass(String pluginType, String pluginName, String pluginId,
                                     PluginProperties pluginProperties, PluginSelector pluginSelector) {
    return pluginConfigurer.usePluginClass(pluginType, pluginName, pluginId, pluginProperties, pluginSelector);
  }
}