package co.cask.mmds.data;

/**
 * Split states.
 */
public enum SplitStatus {
  SPLITTING("Splitting"),
  COMPLETE("Complete"),
  FAILED("Failed");
  private final String label;

  SplitStatus(String label) {
    this.label = label;
  }

  @Override
  public String toString() {
    return label;
  }
}
