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
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import com.google.common.collect.ImmutableList;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Transforms rows using wrangler.
 */
public class WranglerFunction implements FlatMapFunction<Row, Row> {
  private static final Schema TEXT_SCHEMA =
    Schema.recordOf("textRecord", Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
  private final PluginContext pluginContext;
  private final Schema schema;
  private final ServiceDiscoverer serviceDiscoverer;
  private transient TranslatingEmitter emitter;
  private transient Transform<StructuredRecord, StructuredRecord> wrangler;

  public WranglerFunction(Schema schema, PluginContext pluginContext,
                          ServiceDiscoverer serviceDiscoverer) {
    this.schema = schema;
    this.pluginContext = pluginContext;
    this.serviceDiscoverer = serviceDiscoverer;
  }

  @Override
  public Iterator<Row> call(Row input) throws Exception {
    if (wrangler == null) {
      TransformContext context = new WranglerContext(pluginContext, serviceDiscoverer);
      wrangler = pluginContext.newPluginInstance("wrangler");
      wrangler.initialize(context);
      emitter = new TranslatingEmitter(DataFrames.toDataType(schema));
    }

    StructuredRecord record = StructuredRecord.builder(TEXT_SCHEMA).set("body", input.get(0)).build();
    emitter.reset();
    wrangler.transform(record, emitter);
    return emitter.getRecords().iterator();
  }

  /**
   * Transforms emitted StructuredRecords into Spark Rows.
   */
  private static class TranslatingEmitter implements Emitter<StructuredRecord> {
    private final List<Row> outputRecords;
    private final StructType sparkSchema;

    private TranslatingEmitter(StructType sparkSchema) {
      this.sparkSchema = sparkSchema;
      this.outputRecords = new ArrayList<>();
    }

    @Override
    public void emit(StructuredRecord record) {
      outputRecords.add(DataFrames.toRow(record, sparkSchema));
    }

    @Override
    public void emitAlert(Map<String, String> map) {
      // no-op
    }

    @Override
    public void emitError(InvalidEntry<StructuredRecord> invalidEntry) {
      // no-op
    }

    private void reset() {
      outputRecords.clear();
    }

    private List<Row> getRecords() {
      return ImmutableList.copyOf(outputRecords);
    }
  }
}
