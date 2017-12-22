package co.cask.mmds.proto;

import co.cask.mmds.api.Modeler;
import co.cask.mmds.modeler.Modelers;
import com.google.common.base.Joiner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Request to train a model.
 */
public class TrainModelRequest {
  private final String algorithm;
  private final String predictionsDataset;
  private final Map<String, String> hyperparameters;

  public TrainModelRequest(String algorithm, String predictionsDataset, Map<String, String> hyperparameters) {
    this.algorithm = algorithm;
    this.predictionsDataset = predictionsDataset;
    this.hyperparameters = hyperparameters;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  @Nullable
  public String getPredictionsDataset() {
    return predictionsDataset;
  }

  public Map<String, String> getHyperparameters() {
    return Collections.unmodifiableMap(hyperparameters == null ? new HashMap<>() : hyperparameters);
  }

  /**
   * Validate the request this. This is used because this is normally created through deserializing user input,
   * which may be missing fields.
   *
   * @throws BadRequestException if the request is invalid
   */
  public void validate() {
    if (algorithm == null || algorithm.isEmpty()) {
      throw new BadRequestException("Must specify a name");
    }

    Modeler modeler = Modelers.getModeler(algorithm);
    if (modeler == null) {
      throw new BadRequestException(
        String.format("No modeler found for algorithm '%s'. Must be one of '%s'",
                      algorithm, Joiner.on(',').join(Modelers.getAlgorithms())));
    }
  }
}
