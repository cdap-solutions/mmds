package co.cask.mmds.modeler.param.spec;

import java.util.Map;

/**
 * A long Modeler parameter.
 */
public class LongParam extends Param<Long> {
  private final ParamSpec spec;

  public LongParam(String name, String description, long defaultVal, Range range, Map<String, String> params) {
    super(name, description, defaultVal, params);
    spec = new ParamSpec("long", name, description, String.valueOf(defaultVal), null, range);
  }

  protected Long parseVal(String strVal) {
    try {
      return Long.parseLong(strVal);
    } catch (NumberFormatException e) {
      throw new NumberFormatException(
        String.format("Invalid modeler parameter %s=%s. Must be a valid long.", name, strVal));
    }
  }

  @Override
  public ParamSpec getSpec() {
    return spec;
  }
}
