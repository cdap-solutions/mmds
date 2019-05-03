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

package io.cdap.mmds.modeler.train;

import io.cdap.cdap.api.data.schema.Schema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.cdap.mmds.api.AlgorithmType;
import io.cdap.mmds.api.AlgorithmType;
import io.cdap.mmds.data.EvaluationMetrics;
import io.cdap.mmds.data.EvaluationMetrics;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.util.MLWritable;
import org.apache.spark.sql.Dataset;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * The result of training a model. Includes various components that can be saved.
 */
public class ModelOutput {
  private final PipelineModel featureGenModel;
  private final MLWritable model;
  private final EvaluationMetrics evaluationMetrics;
  private final List<String> featureNames;
  private final Set<String> categoricalFeatures;
  private final Dataset predictions;
  private final StringIndexerModel targetIndexModel;
  private final AlgorithmType algorithmType;
  private final Schema schema;

  private ModelOutput(PipelineModel featureGenModel, MLWritable model, EvaluationMetrics evaluationMetrics,
                      List<String> featureNames, Set<String> categoricalFeatures,
                      Dataset predictions, @Nullable StringIndexerModel targetIndexModel,
                      AlgorithmType algorithmType, Schema schema) {
    this.featureGenModel = featureGenModel;
    this.model = model;
    this.evaluationMetrics = evaluationMetrics;
    this.featureNames = ImmutableList.copyOf(featureNames);
    this.categoricalFeatures = ImmutableSet.copyOf(categoricalFeatures);
    this.predictions = predictions;
    this.targetIndexModel = targetIndexModel;
    this.algorithmType = algorithmType;
    this.schema = schema;
  }

  public PipelineModel getFeatureGenModel() {
    return featureGenModel;
  }

  public MLWritable getModel() {
    return model;
  }

  public EvaluationMetrics getEvaluationMetrics() {
    return evaluationMetrics;
  }

  public List<String> getFeatureNames() {
    return featureNames;
  }

  public Set<String> getCategoricalFeatures() {
    return categoricalFeatures;
  }

  public Dataset getPredictions() {
    return predictions;
  }

  @Nullable
  public StringIndexerModel getTargetIndexModel() {
    return targetIndexModel;
  }

  public AlgorithmType getAlgorithmType() {
    return algorithmType;
  }

  public Schema getSchema() {
    return schema;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds model output.
   */
  public static class Builder {
    private PipelineModel featureGenModel;
    private MLWritable model;
    private EvaluationMetrics evaluationMetrics;
    private List<String> featureNames;
    private Set<String> categoricalFeatures;
    private StringIndexerModel targetIndexModel;
    private Dataset predictions;
    private AlgorithmType algorithmType;
    private Schema schema;

    public Builder() {
      featureNames = new ArrayList<>();
      categoricalFeatures = new HashSet<>();
    }

    public Builder setFeatureGenModel(PipelineModel featureGenModel) {
      this.featureGenModel = featureGenModel;
      return this;
    }

    public Builder setModel(MLWritable model) {
      this.model = model;
      return this;
    }

    public Builder setEvaluationMetrics(EvaluationMetrics evaluationMetrics) {
      this.evaluationMetrics = evaluationMetrics;
      return this;
    }

    public Builder setFeatureNames(List<String> featureNames) {
      this.featureNames.clear();
      this.featureNames.addAll(featureNames);
      return this;
    }

    public Builder setCategoricalFeatures(Set<String> categoricalFeatures) {
      this.categoricalFeatures.clear();
      this.categoricalFeatures.addAll(categoricalFeatures);
      return this;
    }

    public Builder setTargetIndexModel(StringIndexerModel targetIndexModel) {
      this.targetIndexModel = targetIndexModel;
      return this;
    }

    public Builder setPredictions(Dataset predictions) {
      this.predictions = predictions;
      return this;
    }

    public Builder setAlgorithmType(AlgorithmType algorithmType) {
      this.algorithmType = algorithmType;
      return this;
    }

    public Builder setSchema(Schema schema) {
      this.schema = schema;
      return this;
    }

    public ModelOutput build() {
      // none of these should be null unless there is a bug in our code.
      if (featureGenModel == null) {
        throw new IllegalStateException("FeatureGen pipeline is not set.");
      }
      if (model == null) {
        throw new IllegalStateException("Model is not set.");
      }
      if (evaluationMetrics == null) {
        throw new IllegalStateException("Model evaluation metrics are not set.");
      }
      if (predictions == null) {
        throw new IllegalStateException("Test predictions not set.");
      }
      if (algorithmType == null) {
        throw new IllegalStateException("Algorithm type not set.");
      }
      if (schema == null) {
        throw new IllegalStateException("Schema not set.");
      }
      return new ModelOutput(featureGenModel, model, evaluationMetrics, featureNames, categoricalFeatures,
                             predictions, targetIndexModel, algorithmType, schema);
    }
  }
}
