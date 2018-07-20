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

package co.cask.mmds.stats;

import java.io.Serializable;

/**
 * Histogram.
 */
public abstract class Histogram<T extends Histogram> implements Serializable {
  private static final long serialVersionUID = -4477404207939331843L;
  protected long totalCount;
  protected long nullCount;

  protected Histogram(long totalCount, long nullCount) {
    this.totalCount = totalCount;
    this.nullCount = nullCount;
  }

  public long getTotalCount() {
    return totalCount;
  }

  public long getNullCount() {
    return nullCount;
  }

  public long getNonNullCount() {
    return totalCount - nullCount;
  }

  public abstract T merge(T other);
}
