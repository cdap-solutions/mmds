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

package io.cdap.mmds.modeler.feature;

import io.cdap.mmds.Constants;
import io.cdap.mmds.Constants;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Generates the feature generation pipeline model and saves it before using it to generate features.
 * Used in model trainers.
 */
public class FeatureGeneratorTrainer extends FeatureGenerator {

  public FeatureGeneratorTrainer(List<String> features, Set<String> categoricalFeatures) {
    super(features, categoricalFeatures);
  }

  @Override
  protected PipelineModel getFeatureGenModel(Dataset<Row> cleanData) {
    // assume there are the original features and cleaned copies of those features: x, y, z, _c_x, _c_y, _c_z
    // we add columns instead of modifying in place because the predictor needs to preserve original values
    // cleaned columns will have nulls replaced, with numeric columns using -1 and categorical using "?"
    List<PipelineStage> stages = new ArrayList<>();
    // index categorical features (assign double values to all strings). If x and z are categorical:
    // x, y, z, _c_x, _c_y, _c_z, _i_x, _i_z
    List<String> indexedCategoricalFeatures = new ArrayList<>();
    for (String feature : features) {
      if (!isCategorical(feature)) {
        continue;
      }
      String cleanName = cleanName(feature);
      String indexedName = indexedName(feature);
      // skip means that if there is a new category in the data to predict, that row will be skipped
      // for example, training data only had values 'A', 'B', and 'C', but the data now contains new value 'D'.
      // the alternative is error, which throws an exception that kills everything
      stages.add(new StringIndexer().setInputCol(cleanName).setOutputCol(indexedName).setHandleInvalid("skip"));
      indexedCategoricalFeatures.add(indexedName);
    }

    // add column has has all features assembled in a vector
    // x, y, z, _c_x, _c_y, _c_z, _i_x, _i_z, _features
    List<String> finalFeatureFields = new ArrayList<>();
    for (String featureName : features) {
      finalFeatureFields.add(isCategorical(featureName) ? indexedName(featureName) : cleanName(featureName));
      if (isCategorical(featureName)) {
        continue;
      }
      finalFeatureFields.add(cleanName(featureName));
    }
    stages.add(new VectorAssembler()
                 .setInputCols(finalFeatureFields.toArray(new String[finalFeatureFields.size()]))
                 .setOutputCol(Constants.FEATURES_FIELD));

    Pipeline featureGenPipeline = new Pipeline().setStages(stages.toArray(new PipelineStage[stages.size()]));
    return featureGenPipeline.fit(cleanData);
  }
}
