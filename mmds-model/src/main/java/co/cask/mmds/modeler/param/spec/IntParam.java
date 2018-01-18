package co.cask.mmds.modeler.param.spec;

import java.util.Map;

/**
 * An integer Modeler parameter.
 */
public class IntParam extends Param<Integer> {
  private final ParamSpec spec;

  public IntParam(String name, String description, int defaultVal, Range range, Map<String, String> params) {
    super(name, description, defaultVal, params);
    spec = new ParamSpec("int", name, description, String.valueOf(defaultVal), null, range);
  }

  @Override
  protected Integer parseVal(String strVal) {
    try {
      return Integer.parseInt(strVal);
    } catch (NumberFormatException e) {
      throw new NumberFormatException(
        String.format("Invalid modeler parameter %s=%s. Must be a valid integer.", name, strVal));
    }
  }

  @Override
  public ParamSpec getSpec() {
    return spec;
  }
}
