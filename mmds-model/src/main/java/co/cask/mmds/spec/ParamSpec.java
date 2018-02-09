package co.cask.mmds.spec;

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
  private final String label;
  private final String description;
  private final String defaultVal;
  private final Set<String> validValues;
  private final Range range;

  public ParamSpec(String type, String name, String label, String description, String defaultVal,
                   @Nullable Set<String> validValues, @Nullable Range range) {
    this.type = type;
    this.name = name;
    this.label = label;
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

  public String getLabel() {
    return label;
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
