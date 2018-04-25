package co.cask.mmds;

/**
 * Constants for ML plugins.
 */
public class Constants {

  public static final String TRAINER_PREDICTION_FIELD = "_prediction";
  public static final String FEATURES_FIELD = "_features";

  /**
   * Modeling components
   */
  public static class Component {
    public static final String FEATUREGEN = "featuregen";
    public static final String MODEL = "model";
    public static final String TARGET_INDICES = "targetindices";
  }

  /**
   * Default dataset names
   */
  public static class Dataset {
    public static final String EXPERIMENTS_META = "experiment_meta";
    public static final String MODEL_META = "experiment_model_meta";
    public static final String MODEL_COMPONENTS = "experiment_model_components";
    public static final String SPLITS = "experiment_splits";
  }
}
