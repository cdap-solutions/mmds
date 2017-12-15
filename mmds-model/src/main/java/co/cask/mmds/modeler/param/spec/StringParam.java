package co.cask.mmds.modeler.param.spec;

import java.util.Map;
import java.util.Set;

/**
 * A String Modeler parameter.
 */
public class StringParam extends Param<String> {
  private final ParamSpec spec;

  public StringParam(String name, String description, String defaultVal, Set<String> validValues,
                     Map<String, String> params) {
    super(name, description, defaultVal, params);
    spec = new ParamSpec("string", name, description, defaultVal, validValues, null);
  }

  protected String parseVal(String strVal) {
    return strVal;
  }

  @Override
  public ParamSpec getSpec() {
    return spec;
  }
}
