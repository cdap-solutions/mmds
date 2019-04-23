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

package io.cdap.mmds.splitter.param;

import io.cdap.mmds.spec.*;
import io.cdap.mmds.spec.IntParam;
import io.cdap.mmds.spec.LongParam;
import io.cdap.mmds.spec.ParamSpec;
import io.cdap.mmds.spec.Parameters;
import io.cdap.mmds.spec.Params;
import io.cdap.mmds.spec.Range;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Parameters for random splitter.
 */
public class RandomParams implements Parameters {
  private final IntParam testPercentage;
  private final LongParam seed;

  public RandomParams(Map<String, String> properties) {
    this.testPercentage = new IntParam("testPercentage", "Test Percentage",
                                       "Percent of data that should be used for testing. " +
                                         "Must be a whole number and greater than 0 but less than 100.",
                                       20, new Range(1, 100, true, false), properties);
    this.seed = new LongParam("seed", "Seed",
                              "Seed to use for random splitting of data. The same seed, " +
                                "same test percentage, and same data will produce the same splits.",
                              null, null, properties);
  }

  public double[] getWeights() {
    return new double[] { 100 - testPercentage.getVal(), testPercentage.getVal() };
  }

  @Nullable
  public Long getSeed() {
    return seed.getVal();
  }

  @Override
  public Map<String, String> toMap() {
    Map<String, String> map = new HashMap<>();
    return Params.putParams(map, testPercentage, seed);
  }

  @Override
  public List<ParamSpec> getSpec() {
    List<ParamSpec> spec = new ArrayList<>();
    return Params.addParams(spec, testPercentage, seed);
  }
}
