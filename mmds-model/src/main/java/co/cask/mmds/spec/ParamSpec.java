/*
 * Copyright © 2017-2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

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
