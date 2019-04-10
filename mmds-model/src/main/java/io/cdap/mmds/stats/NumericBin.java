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

package io.cdap.mmds.stats;


import java.io.Serializable;

/**
 * A numeric histogram bin.
 */
public class NumericBin implements Serializable {
  private static final long serialVersionUID = -4458231951109307443L;
  private final double lo;
  private final double hi;
  private final boolean hiInclusive;
  private long count;

  public NumericBin(double lo, double hi, long count, boolean hiInclusive) {
    this.lo = lo;
    this.hi = hi;
    this.count = count;
    this.hiInclusive = hiInclusive;
  }

  public NumericBin merge(NumericBin other) {
    if (lo != other.lo || hi != other.hi || hiInclusive != other.hiInclusive) {
      throw new IllegalArgumentException("Cannot merge histograms with different bins.");
    }
    return new NumericBin(lo, hi, count + other.count, hiInclusive);
  }

  public boolean incrementIfInBin(Double val) {
    if (val == null) {
      return false;
    }
    if (val < lo) {
      return false;
    }
    if (val == hi && hiInclusive) {
      count++;
      return true;
    } else if (val < hi) {
      count++;
      return true;
    }
    return false;
  }

  public double getLo() {
    return lo;
  }

  public double getHi() {
    return hi;
  }

  public long getCount() {
    return count;
  }

  public boolean isHiInclusive() {
    return hiInclusive;
  }

  @Override
  public String toString() {
    return "NumericBin{" +
      "lo=" + lo +
      ", hi=" + hi +
      ", hiInclusive=" + hiInclusive +
      ", count=" + count +
      '}';
  }
}
