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

import java.util.Map;
import java.util.Set;

/**
 * A String Modeler parameter.
 */
public class StringParam extends Param<String> {
  private final ParamSpec spec;

  public StringParam(String name, String label, String description, String defaultVal, Set<String> validValues,
                     Map<String, String> params) {
    super(name, description, defaultVal, params);
    spec = new ParamSpec("string", name, label, description, defaultVal, validValues, null);
  }

  @Override
  protected String parseVal(String strVal) {
    return strVal;
  }

  @Override
  public ParamSpec getSpec() {
    return spec;
  }
}
