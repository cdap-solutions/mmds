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

package io.cdap.mmds.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.spark.sql.DataFrames;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;


/**
 * Converts a dataframe Row to a StructuredRecord.
 */
public class RowToRecord implements Function<Row, StructuredRecord> {
  private final Schema schema;

  public RowToRecord(Schema schema) {
    this.schema = schema;
  }

  @Override
  public StructuredRecord call(Row row) throws Exception {
    return DataFrames.fromRow(row, schema);
  }
}