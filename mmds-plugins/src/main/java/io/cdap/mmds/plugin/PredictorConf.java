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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;

import java.io.IOException;

/**
 * ML Predictor configuration.
 */
@SuppressWarnings("unused")
public class PredictorConf extends PluginConfig {

  @Macro
  @Description("The ID of the experiment the model belongs to.")
  private String experimentId;

  @Macro
  @Description("The ID of the model to use for predictions. Model IDs are unique within an experiment.")
  private String modelId;

  @Macro
  @Description("The field in the output schema to place the prediction. Must be a double for regression models. " +
    "For classifier models, the prediction field can be a double or a string. " +
    "During the process of classifier model training, outcome fields will be assigned a unique double. " +
    "For example, the value 'sports' might be assigned value 0.0, and the value 'news' might be assigned value 1.0. " +
    "If you would like the prediction to use the original string value, make it of type string. " +
    "Otherwise, it should be of type double.")
  private String predictionField;

  @Macro
  @Description("The output schema, which must include the prediction field. " +
    "Must only contain fields from the input schema and the new prediction field.")
  private String schema;

  // to set default values
  public PredictorConf() {
    super();
  }

  public String getPredictionField() {
    return predictionField;
  }

  public String getExperimentID() {
    return experimentId;
  }

  public String getModelID() {
    return modelId;
  }

  public Schema getOutputSchema() {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema.");
    }
  }

  public void validate(Schema inputSchema) {
    boolean outputSchemaIsMacro = containsMacro("outputSchema");
    boolean predictionFieldIsMacro = containsMacro("predictionField");

    if (!predictionFieldIsMacro && inputSchema.getField(predictionField) != null) {
      throw new IllegalArgumentException(
        String.format("Prediction field '%s' already exists in the input schema. " +
                        "Please provide a different prediction field name.", predictionField));
    }

    if (!outputSchemaIsMacro) {
      Schema outputSchema = getOutputSchema();
      if (!predictionFieldIsMacro) {
        Schema.Field predictionSchemaField = outputSchema.getField(predictionField);
        if (predictionSchemaField == null) {
          throw new IllegalArgumentException(
            String.format("Prediction field '%s' does not exist in the output schema. " +
                            "Please add a field for the prediction.", predictionField));
        }
        Schema predictionSchema = predictionSchemaField.getSchema();
        predictionSchema = predictionSchema.isNullable() ? predictionSchema.getNonNullable() : predictionSchema;
        Schema.Type predictionType = predictionSchema.getType();
        if (predictionType != Schema.Type.DOUBLE && predictionType != Schema.Type.STRING) {
          throw new IllegalArgumentException(
            String.format("Prediction field '%s' is of invalid getType '%s'. Must be a double or a string.",
                          predictionField, predictionType));
        }
        // check all non-prediction fields in the output are the same as those in the input
        for (Schema.Field outputField : outputSchema.getFields()) {
          String fieldName = outputField.getName();
          if (fieldName.equals(predictionField)) {
            continue;
          }
          Schema.Field inputField = inputSchema.getField(fieldName);
          if (inputField == null) {
            throw new IllegalArgumentException(
              String.format("Feature '%s' in the output schema is not in the input schema.", fieldName));
          }
          if (!inputField.getSchema().equals(outputField.getSchema())) {
            throw new IllegalArgumentException(
              String.format("Feature '%s' has a different schema in the output than in the input. " +
                              "Please ensure they have the same schema, including whether they are nullable or not.",
                            fieldName));
          }
        }
      }
    }
  }

}
