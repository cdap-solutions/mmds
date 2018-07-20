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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A boolean Modeler parameter.
 */
public class BoolParam extends Param<Boolean> {
  private final ParamSpec spec;

  public BoolParam(String name, String label, String description, boolean defaultVal, Map<String, String> params) {
    super(name, description, defaultVal, params);
    Set<String> validValues = new HashSet<>();
    validValues.add("true");
    validValues.add("false");
    this.spec = new ParamSpec("bool", name, label, description, String.valueOf(defaultVal), validValues, null);
  }

  @Override
  protected Boolean parseVal(String strVal) {
    return Boolean.parseBoolean(strVal);
  }

  @Override
  public ParamSpec getSpec() {
    return spec;
  }
}
