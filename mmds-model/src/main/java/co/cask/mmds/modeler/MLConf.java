package co.cask.mmds.modeler;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.mmds.Constants;

import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Common ML configuration.
 */
@SuppressWarnings("unused")
public class MLConf extends PluginConfig {
  private static final Pattern PATTERN = Pattern.compile("[\\.a-zA-Z0-9_-]+");

  @Macro
  @Description("The ID of the experiment the model belongs to.")
  private String experimentId;

  @Macro
  @Description("The ID of the model to use for predictions. Model IDs are unique within an experiment.")
  private String modelId;

  @Macro
  @Nullable
  @Description("The name of the fileset that stores trained models. Defaults to 'models'. " +
    "MLPredictor plugins should set this to the same value used by the ModelTrainer that trained the model.")
  private String modelDataset;

  @Macro
  @Nullable
  @Description("The name of the table that stores model metadata. Defaults to 'model_meta'. " +
    "MLPredictor plugins should set this to the same value used by the ModelTrainer that trained the model.")
  private String modelMetaDataset;

  // to set default values
  protected MLConf() {
    modelDataset = Constants.Dataset.MODEL_COMPONENTS;
    modelMetaDataset = Constants.Dataset.MODEL_META;
  }

  public String getModelDataset() {
    return modelDataset;
  }

  public String getModelMetaDataset() {
    return modelMetaDataset;
  }

  public String getExperimentID() {
    return experimentId;
  }

  public String getModelID() {
    return modelId;
  }

  public void validate() {
    if (!containsMacro("modelMetaDataset") && !containsMacro("modelDataset") && modelMetaDataset.equals(modelDataset)) {
      throw new IllegalArgumentException("The model dataset and model meta dataset cannot have the same name.");
    }
  }

}