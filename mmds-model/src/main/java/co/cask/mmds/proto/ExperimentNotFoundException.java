package co.cask.mmds.proto;

/**
 * Indicates an experiment was not found.
 */
public class ExperimentNotFoundException extends NotFoundException {

  public ExperimentNotFoundException(String experimentName) {
    super(String.format("Experiment '%s' not found.", experimentName));
  }
}
