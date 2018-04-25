package co.cask.mmds.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.mmds.modeler.MLConf;

import javax.annotation.Nullable;

/**
 * Model Trainer configuration.
 */
public class TrainerConf extends MLConf {

  @Macro
  @Nullable
  @Description("Whether to overwrite an existing model if it already exists. " +
    "If false, an error is thrown if there is an existing model. Defaults to false.")
  private Boolean overwrite;

  @Macro
  @Description("The outcome field. If the field is a string or a boolean, it will be treated as a category. " +
    "If a record has a null outcome field, it will be dropped and will not be used for training the model.")
  private String outcomeField;

  @Macro
  @Description("The modeling algorithm to use to train the model.")
  private String algorithm;

  @Macro
  @Nullable
  @Description("The name of the fileset that stores the predictions computed for the test data. " +
    "If not specified, the predictions will not be stored. The predictions dataset is partitioned by the experiment " +
    "and model ids, but it has a single schema for exploration through sql queries. " +
    "This means that all models that use the same predictions dataset must use the same features of the same getType, " +
    "or exploration will not work correctly. Predictions will be written out in csv format")
  private String predictionsDataset;

  @Macro
  @Nullable
  @Description("The delimiter used to separate fields when writing to the predictions dataset. " +
    "Defaults to a comma.")
  private String predictionsDatasetDelimiter;

  @Macro
  @Nullable
  @Description("Map of training parameters. Different modeling algorithms use different parameters.")
  private String trainingParameters;

  // to set default values
  public TrainerConf() {
    super();
    overwrite = false;
    predictionsDatasetDelimiter = ",";
  }

  @Nullable
  public Boolean getOverwrite() {
    return overwrite;
  }

  @Nullable
  public String getPredictionsDataset() {
    return predictionsDataset;
  }

  public String getPredictionsDatasetDelimiter() {
    return predictionsDatasetDelimiter;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public Boolean shouldOverwrite() {
    return overwrite;
  }

}