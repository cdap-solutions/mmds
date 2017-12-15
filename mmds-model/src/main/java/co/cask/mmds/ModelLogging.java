package co.cask.mmds;

import org.slf4j.MDC;

/**
 * Sets MDC keys to tag log statements for a model.
 */
public class ModelLogging {
  private static final String MODEL_KEY = "MDC:model";
  private static final String EXPERIMENT_KEY = "MDC:experiment";

  public static void start(String experiment, String model) {
    MDC.put(MODEL_KEY, model);
    MDC.put(EXPERIMENT_KEY, experiment);
  }

  public static void finish() {
    MDC.remove(MODEL_KEY);
    MDC.remove(EXPERIMENT_KEY);
  }
}
