package co.cask.mmds;

import org.slf4j.MDC;

/**
 * Sets MDC keys to tag log statements for a data split.
 */
public class SplitLogging {
  private static final String SPLIT_KEY = "MDC:split";
  private static final String EXPERIMENT_KEY = "MDC:experiment";

  public static void start(String experiment, String split) {
    MDC.put(SPLIT_KEY, split);
    MDC.put(EXPERIMENT_KEY, experiment);
  }

  public static void finish() {
    MDC.remove(SPLIT_KEY);
    MDC.remove(EXPERIMENT_KEY);
  }
}
