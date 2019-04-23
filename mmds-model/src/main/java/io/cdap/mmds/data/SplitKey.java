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

package io.cdap.mmds.data;


import java.util.Objects;

/**
 * Key for a split.
 */
public class SplitKey {
  private final String experiment;
  private final String split;

  public SplitKey(String experiment, String split) {
    this.experiment = experiment;
    this.split = split;
  }

  public String getExperiment() {
    return experiment;
  }

  public String getSplit() {
    return split;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SplitKey that = (SplitKey) o;

    return Objects.equals(experiment, that.experiment) && Objects.equals(split, that.split);
  }

  @Override
  public int hashCode() {
    return Objects.hash(experiment, split);
  }

  @Override
  public String toString() {
    return "ModelKey{" +
      "experiment='" + experiment + '\'' +
      ", split='" + split + '\'' +
      '}';
  }
}
