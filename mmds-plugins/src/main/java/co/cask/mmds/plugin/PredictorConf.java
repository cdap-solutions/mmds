package co.cask.mmds.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.mmds.modeler.MLConf;

import java.io.IOException;

/**
 * ML Predictor configuration.
 */
@SuppressWarnings("unused")
public class PredictorConf extends MLConf {

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

  public Schema getOutputSchema() {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema.");
    }
  }

  public void validate(Schema inputSchema) {
    super.validate();

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