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
 * A bin in a histogram.
 */
public class HistogramBin {
  private final String bin;
  private final long count;

  public HistogramBin(String bin, long count) {
    this.bin = bin;
    this.count = count;
  }

  public String getBin() {
    return bin;
  }

  public long getCount() {
    return count;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HistogramBin that = (HistogramBin) o;

    return Objects.equals(bin, that.bin) && Objects.equals(count, that.count);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bin, count);
  }
}
