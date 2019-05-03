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


import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metadata.Metadata;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.Arguments;
import io.cdap.cdap.etl.api.Lookup;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;

import java.net.URL;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

// TODO: remove usage of Hydrator classes. Requires wrangler work

/**
 * Implements TransformContext required by the WranglerTransform.
 */
public class WranglerContext implements TransformContext {
  private final PluginContext pluginContext;
  private final ServiceDiscoverer serviceDiscoverer;

  public WranglerContext(PluginContext pluginContext, ServiceDiscoverer serviceDiscoverer) {
    this.pluginContext = pluginContext;
    this.serviceDiscoverer = serviceDiscoverer;
  }

  @Override
  public <T> Lookup<T> provide(String s, Map<String, String> map) {
    throw new UnsupportedOperationException("Lookup not supported in MMDS.");
  }

  @Override
  public String getStageName() {
    return "stage";
  }

  @Override
  public String getNamespace() {
    return "default";
  }

  @Override
  public String getPipelineName() {
    return "pipeline";
  }

  @Override
  public long getLogicalStartTime() {
    return 0;
  }

  @Override
  public StageMetrics getMetrics() {
    return new StageMetrics() {
      @Override
      public void count(String s, int i) {
        // no-op
      }

      @Override
      public void gauge(String s, long l) {
        // no-op
      }

      @Override
      public void pipelineCount(String s, int i) {
        // no-op
      }

      @Override
      public void pipelineGauge(String s, long l) {
        // no-op
      }
    };
  }

  @Override
  public PluginProperties getPluginProperties() {
    return pluginContext.getPluginProperties("wrangler");
  }

  @Override
  public PluginProperties getPluginProperties(String s) {
    return pluginContext.getPluginProperties(s);
  }

  @Override
  public <T> Class<T> loadPluginClass(String s) {
    return pluginContext.loadPluginClass(s);
  }

  @Override
  public <T> T newPluginInstance(String s) throws InstantiationException {
    return pluginContext.newPluginInstance("s");
  }

  @Nullable
  @Override
  public Schema getInputSchema() {
    return null;
  }

  @Override
  public Map<String, Schema> getInputSchemas() {
    return Collections.emptyMap();
  }

  @Nullable
  @Override
  public Schema getOutputSchema() {
    return null;
  }

  @Override
  public Map<String, Schema> getOutputPortSchemas() {
    return Collections.emptyMap();
  }

  @Override
  public Arguments getArguments() {
    return new Arguments() {
      @Override
      public boolean has(String s) {
        return false;
      }

      @Nullable
      @Override
      public String get(String s) {
        return null;
      }

      @Override
      public Iterator<Map.Entry<String, String>> iterator() {
        return Collections.emptyIterator();
      }
    };
  }

  @Nullable
  @Override
  public URL getServiceURL(String applicationId, String serviceId) {
    return serviceDiscoverer.getServiceURL(applicationId, serviceId);
  }

  @Nullable
  @Override
  public URL getServiceURL(String serviceId) {
    return serviceDiscoverer.getServiceURL(serviceId);
  }

  @Override
  public Map<MetadataScope, Metadata> getMetadata(MetadataEntity metadataEntity) {
    return Collections.emptyMap();
  }

  @Override
  public Metadata getMetadata(MetadataScope metadataScope, MetadataEntity metadataEntity) {
    return new Metadata(Collections.emptyMap(), Collections.emptySet());
  }

  @Override
  public void addProperties(MetadataEntity metadataEntity, Map<String, String> map) {
    // no-op
  }

  @Override
  public void addTags(MetadataEntity metadataEntity, String... strings) {
    // no-op
  }

  @Override
  public void addTags(MetadataEntity metadataEntity, Iterable<String> iterable) {
    // no-op
  }

  @Override
  public void removeMetadata(MetadataEntity metadataEntity) {
    // no-op
  }

  @Override
  public void removeProperties(MetadataEntity metadataEntity) {
    // no-op
  }

  @Override
  public void removeProperties(MetadataEntity metadataEntity, String... strings) {
    // no-op
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity) {
    // no-op
  }

  @Override
  public void removeTags(MetadataEntity metadataEntity, String... strings) {
    // no-op
  }

  @Override
  public void record(List<FieldOperation> fieldOperations) {
    // no-op
  }
}
