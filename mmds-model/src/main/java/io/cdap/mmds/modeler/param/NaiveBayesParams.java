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

package io.cdap.mmds.modeler.param;

import com.google.common.collect.ImmutableSet;
import io.cdap.mmds.spec.DoubleParam;
import io.cdap.mmds.spec.ParamSpec;
import io.cdap.mmds.spec.Parameters;
import io.cdap.mmds.spec.Params;
import io.cdap.mmds.spec.Range;
import io.cdap.mmds.spec.StringParam;
import org.apache.spark.ml.classification.NaiveBayes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Modeler parameters for Naive Bayes.
 */
public class NaiveBayesParams implements Parameters {
  private final DoubleParam smoothing;
  private final StringParam type;

  public NaiveBayesParams(Map<String, String> modelParams) {
    smoothing = new DoubleParam("smoothing", "Smoothing", "smoothing parameter", 1.0d, new Range(0, true), modelParams);
    type = new StringParam("type", "Type", "model type", "multinomial",
                           ImmutableSet.of("multinomial", "bernoulli"), modelParams);
  }

  public void setParams(NaiveBayes modeler) {
    modeler.setSmoothing(smoothing.getVal());
    modeler.setModelType(type.getVal());
  }

  @Override
  public Map<String, String> toMap() {
    return Params.putParams(new HashMap<>(), smoothing, type);
  }

  @Override
  public List<ParamSpec> getSpec() {
    return Params.addParams(new ArrayList<>(), smoothing, type);
  }
}
