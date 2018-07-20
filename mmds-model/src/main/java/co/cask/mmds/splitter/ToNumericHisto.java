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

package co.cask.mmds.splitter;

import co.cask.mmds.stats.NumericHisto;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Map;

/**
 * Converts (column, value) to (column, histogram).
 */
public class ToNumericHisto implements PairFunction<Tuple2<String, Double>, String, NumericHisto> {
  private final Map<String, Tuple2<Double, Double>> columnMinMax;

  public ToNumericHisto(Map<String, Tuple2<Double, Double>> columnMinMax) {
    this.columnMinMax = columnMinMax;
  }

  @Override
  public Tuple2<String, NumericHisto> call(Tuple2<String, Double> input) throws Exception {
    Tuple2<Double, Double> minMax = columnMinMax.get(input._1());
    return new Tuple2<>(input._1(), new NumericHisto(minMax._1(), minMax._2(), 10, input._2()));
  }
}
