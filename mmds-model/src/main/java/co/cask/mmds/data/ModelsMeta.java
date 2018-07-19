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

package co.cask.mmds.data;

import java.util.List;

/**
 * Holds information about total row count of models table along with list of models.
 */
public class ModelsMeta {
  private final long totalRowCount;
  private final List<ModelMeta> models;

  public ModelsMeta(long totalRowCount, List<ModelMeta> models) {
    this.totalRowCount = totalRowCount;
    this.models = models;
  }

  public long getTotalRowCount() {
    return totalRowCount;
  }

  public List<ModelMeta> getModels() {
    return models;
  }
}
