package co.cask.mmds.manager;


import co.cask.cdap.api.ServiceDiscoverer;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginContext;
import co.cask.cdap.api.plugin.PluginProperties;
import co.cask.cdap.etl.api.Arguments;
import co.cask.cdap.etl.api.Lookup;
import co.cask.cdap.etl.api.StageMetrics;
import co.cask.cdap.etl.api.TransformContext;

import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
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
    return new HashMap<>();
  }

  @Nullable
  @Override
  public Schema getOutputSchema() {
    return null;
  }

  @Override
  public Map<String, Schema> getOutputPortSchemas() {
    return new HashMap<>();
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
        return null;
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
}