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

package io.cdap.mmds.splitter;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Used to convert a collection of Rows into a collection of tuple2s, where the first element is a column name and the
 * second element is a value for that column.
 */
public class ToDoubleValues implements PairFlatMapFunction<Row, String, Double> {
  private final List<String> columns;

  public ToDoubleValues(List<String> columns) {
    this.columns = new ArrayList<>(columns);
  }

  @Override
  public Iterator<Tuple2<String, Double>> call(Row row) throws Exception {
    List<Tuple2<String, Double>> values = new ArrayList<>();
    for (String column : columns) {
      values.add(new Tuple2<>(column, (Double) row.getAs(column)));
    }
    return values.iterator();
  }
}
