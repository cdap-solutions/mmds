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

package co.cask.mmds.proto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Request to create a model
 */
public class CreateModelRequest {
  private final String name;
  private final String description;
  private final String split;
  private final List<String> directives;

  public CreateModelRequest(String name, String description, List<String> directives) {
    this(name, description, directives, null);
  }

  public CreateModelRequest(String name, String description, List<String> directives, @Nullable String split) {
    this.name = name;
    this.description = description;
    this.directives = Collections.unmodifiableList(directives);
    this.split = split;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public String getSplit() {
    return split;
  }

  public List<String> getDirectives() {
    return directives == null ? new ArrayList<>() : directives;
  }

  /**
   * Validate the request this. This is used because this is normally created through deserializing user input,
   * which may be missing fields.
   *
   * @throws BadRequestException if the request is invalid
   */
  public void validate() {
    if (name == null || name.isEmpty()) {
      throw new BadRequestException("Must specify a name");
    }
    if (split != null && directives != null) {
      throw new BadRequestException("Cannot specify both directives and a split.");
    }
  }
}
