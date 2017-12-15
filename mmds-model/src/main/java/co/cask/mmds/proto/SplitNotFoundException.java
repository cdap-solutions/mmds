package co.cask.mmds.proto;

import co.cask.mmds.data.SplitKey;

/**
 * Indicates a split was not found.
 */
public class SplitNotFoundException extends NotFoundException {

  public SplitNotFoundException(SplitKey splitKey) {
    super(String.format("Split '%s' in experiment '%s' not found.", splitKey.getSplit(), splitKey.getExperiment()));
  }
}
