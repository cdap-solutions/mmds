package co.cask.mmds.modeler.param.spec;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A boolean Modeler parameter.
 */
public class BoolParam extends Param<Boolean> {
  private final ParamSpec spec;

  public BoolParam(String name, String description, boolean defaultVal, Map<String, String> params) {
    super(name, description, defaultVal, params);
    Set<String> validValues = new HashSet<>();
    validValues.add("true");
    validValues.add("false");
    this.spec = new ParamSpec("bool", name, description, String.valueOf(defaultVal), validValues, null);
  }

  protected Boolean parseVal(String strVal) {
    return Boolean.parseBoolean(strVal);
  }

  @Override
  public ParamSpec getSpec() {
    return spec;
  }
}
