package co.cask.mmds.modeler.param;

import co.cask.mmds.modeler.param.spec.ParamSpec;

import java.util.List;
import java.util.Map;

/**
 * Modeler parameters with some utility methods for getting and validating parameters.
 */
public interface ModelerParams {

  /**
   * @return model parameters as a string map
   */
  Map<String, String> toMap();

  /**
   * @return specification for all modeler params
   */
  List<ParamSpec> getSpec();
}
