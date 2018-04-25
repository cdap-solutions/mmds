package co.cask.mmds.proto;

import co.cask.mmds.data.ModelKey;

/**
 * Indicates a model was not found.
 */
public class ModelNotFoundException extends NotFoundException {

  public ModelNotFoundException(ModelKey modelKey) {
    super(String.format("Model '%s' in experiment '%s' not found.", modelKey.getModel(), modelKey.getExperiment()));
  }
}
