package co.cask.mmds.spec;

import java.util.Map;

/**
 * A Modeler parameter.
 */
public abstract class Param<T> {
  protected final String name;
  protected final String description;
  private final T val;

  public Param(String name, String description, T defaultVal, Map<String, String> params) {
    this.name = name;
    this.description = description;
    String strVal = params.get(name);
    this.val = strVal == null ? defaultVal : parseVal(strVal);
  }

  /**
   * Parse the given string as a typed value
   *
   * @param strVal the value as a string
   * @return the parsed value
   */
  protected abstract T parseVal(String strVal);

  /**
   * @return specification for this parameter
   */
  public abstract ParamSpec getSpec();

  public String getName() {
    return name;
  }

  public T getVal() {
    return val;
  }

  public String getValStr() {
    return val.toString();
  }
}
