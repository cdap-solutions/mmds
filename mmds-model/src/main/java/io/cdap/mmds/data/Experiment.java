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

import io.cdap.cdap.api.data.schema.Schema;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Holds information about an Experiment
 */
public class Experiment {
  private static final Set<Schema.Type> VALID_TYPES =
    ImmutableSet.of(Schema.Type.BOOLEAN, Schema.Type.STRING,
                    Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE);
  private final String name;
  private final String description;
  private final String srcpath;
  private final String outcome;
  private final String outcomeType;
  private final List<String> directives;

  public Experiment(String name, Experiment experiment) {
    this(name, experiment.getDescription(), experiment.getSrcpath(),
         experiment.getOutcome(), experiment.getOutcomeType(), experiment.getDirectives());
  }

  public Experiment(String name, String description, String srcpath, String outcome, String outcomeType,
                    List<String> directives) {
    this.name = name;
    this.description = description == null ? "" : description;
    this.srcpath = srcpath;
    this.outcome = outcome;
    this.outcomeType = outcomeType;
    this.directives = Collections.unmodifiableList(directives);
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getSrcpath() {
    return srcpath;
  }

  public String getOutcome() {
    return outcome;
  }

  public String getOutcomeType() {
    return outcomeType;
  }

  public List<String> getDirectives() {
    return directives == null ? Collections.emptyList() : directives;
  }

  public void validate() {
    if (srcpath == null || srcpath.isEmpty()) {
      throw new IllegalArgumentException("Experiment srcpath must be provided.");
    }
    if (outcome == null || outcome.isEmpty()) {
      throw new IllegalArgumentException("Experiment outcome must be provided.");
    }
    if (outcomeType == null || outcomeType.isEmpty()) {
      throw new IllegalArgumentException("Experiment outcomeType must be provided.");
    }
    try {
      Schema.Type type = Schema.Type.valueOf(outcomeType.toUpperCase());
      if (!VALID_TYPES.contains(type)) {
        throw new IllegalArgumentException(String.format("Experiment outcomeType '%s' is invalid. Must be one of '%s'.",
                                                         outcomeType, Joiner.on(',').join(VALID_TYPES)));
      }
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format("Experiment outcomeType '%s' is invalid. Must be one of '%s'.",
                                                       outcomeType, Joiner.on(',').join(VALID_TYPES)));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Experiment that = (Experiment) o;

    return Objects.equals(name, that.name) &&
      Objects.equals(description, that.description) &&
      Objects.equals(srcpath, that.srcpath) &&
      Objects.equals(outcome, that.outcome) &&
      Objects.equals(outcomeType, that.outcomeType) &&
      Objects.equals(directives, that.directives);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, description, srcpath, outcome, outcomeType, directives);
  }

  @Override
  public String toString() {
    return "Experiment{" +
      "name='" + name + '\'' +
      ", description='" + description + '\'' +
      ", srcpath='" + srcpath + '\'' +
      ", outcome='" + outcome + '\'' +
      ", outcomeType='" + outcomeType + '\'' +
      ", directives=" + directives +
      '}';
  }
}
