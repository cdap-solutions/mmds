package co.cask.mmds.spec;

import java.util.Map;

/**
 * A double Modeler parameter.
 */
public class DoubleParam extends Param<Double> {
  private final ParamSpec spec;

  public DoubleParam(String name, String label, String description, double defaultVal,
                     Range range, Map<String, String> params) {
    super(name, description, defaultVal, params);
    spec = new ParamSpec("double", name, label, description, String.valueOf(defaultVal), null, range);
  }

  @Override
  protected Double parseVal(String strVal) {
    try {
      return Double.parseDouble(strVal);
    } catch (NumberFormatException e) {
      throw new NumberFormatException(
        String.format("Invalid modeler parameter %s=%s. Must be a valid double.", name, strVal));
    }
  }

  @Override
  public ParamSpec getSpec() {
    return spec;
  }
}