package co.cask.mmds.spec;

import co.cask.mmds.spec.ParamSpec;

import java.util.List;
import java.util.Map;

/**
 * Parameters for modeling or splitting.
 */
public interface Parameters {

  /**
   * @return model parameters as a string map
   */
  Map<String, String> toMap();

  /**
   * @return specification for all modeler params
   */
  List<ParamSpec> getSpec();
}
