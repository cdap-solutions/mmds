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

package io.cdap.mmds.modeler.feature;


import java.util.Objects;

/**
 * Represents a field.
 */
public class Feature {
  private final String name;
  private final boolean isCategorical;

  public Feature(String name, boolean isCategorical) {
    this.name = name;
    this.isCategorical = isCategorical;
  }

  public String getName() {
    return name;
  }

  public boolean isCategorical() {
    return isCategorical;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Feature that = (Feature) o;

    return Objects.equals(name, that.name) && isCategorical == that.isCategorical;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, isCategorical);
  }
}
