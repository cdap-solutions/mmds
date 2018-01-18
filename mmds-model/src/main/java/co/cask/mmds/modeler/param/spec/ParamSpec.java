package co.cask.mmds.modeler.param.spec;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Specification for a modeler param.
 */
public class ParamSpec {
  private final String type;
  private final String name;
  private final String description;
  private final String defaultVal;
  private final Set<String> validValues;
  private final Range range;

  public ParamSpec(String type, String name, String description, String defaultVal,
                   @Nullable Set<String> validValues, @Nullable Range range) {
    this.name = name;
    this.type = type;
    this.description = description;
    this.defaultVal = defaultVal;
    this.validValues = validValues == null ? new HashSet<>() : Collections.unmodifiableSet(validValues);
    this.range = range;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getDefaultVal() {
    return defaultVal;
  }

  public Set<String> getValidValues() {
    return validValues;
  }

  @Nullable
  public Range getRange() {
    return range;
  }
}
