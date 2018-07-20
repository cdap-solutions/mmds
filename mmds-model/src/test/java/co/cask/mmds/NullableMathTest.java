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

package co.cask.mmds;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for NullableMath.
 */
public class NullableMathTest {

  @Test
  public void testMin() {
    Assert.assertNull(NullableMath.min(null, null));
    Assert.assertEquals(-1.3d, NullableMath.min(-1.3d, null), 0.0000001d);
    Assert.assertEquals(-1.3d, NullableMath.min(null, -1.3d), 0.0000001d);
    Assert.assertEquals(-1.3d, NullableMath.min(-1.3d, 1.3d), 0.0000001d);
  }

  @Test
  public void testMax() {
    Assert.assertNull(NullableMath.min(null, null));
    Assert.assertEquals(-1.3d, NullableMath.max(-1.3d, null), 0.0000001d);
    Assert.assertEquals(-1.3d, NullableMath.max(null, -1.3d), 0.0000001d);
    Assert.assertEquals(1.3d, NullableMath.max(-1.3d, 1.3d), 0.0000001d);
  }

  @Test
  public void testMean() {
    Assert.assertNull(NullableMath.min(null, null));
    Assert.assertEquals(-1.3d, NullableMath.mean(-1.3d, 5L, null, 0L), 0.0000001d);
    Assert.assertEquals(-1.3d, NullableMath.mean(null, 0L, -1.3d, 5L), 0.0000001d);

    double mean1 = (1 + 2 + 3 + 4 + 5) / 5d;
    long count1 = 5;
    double mean2 = (6 + 7 + 8 + 9) / 4d;
    long count2 = 4;

    double expected = (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9) / 9d;
    Assert.assertEquals(expected, NullableMath.mean(mean1, count1, mean2, count2), 0.0000001d);
  }

  @Test
  public void testM2() {
    Assert.assertNull(NullableMath.min(null, null));
    Assert.assertEquals(-1.3d, NullableMath.mean(-1.3d, 5L, null, 0L), 0.0000001d);
    Assert.assertEquals(-1.3d, NullableMath.mean(null, 0L, -1.3d, 5L), 0.0000001d);

    List<Double> vals1 = ImmutableList.of(1d, 2d, 3d, 4d, 5d);
    double total1 = vals1.stream().reduce(0d, Double::sum);
    double mean1 = total1 / vals1.size();
    double m21 = 0d;
    for (Double val : vals1) {
      m21 += (val - mean1) * (val - mean1);
    }

    List<Double> vals2 = ImmutableList.of(6d, 7d, 8d, 9d);
    double total2 = vals2.stream().reduce(0d, Double::sum);
    double mean2 = total2 / vals2.size();
    double m22 = 0d;
    for (Double val : vals2) {
      m22 += (val - mean2) * (val - mean2);
    }

    double meanTotal = (total1 + total2) / (vals1.size() + vals2.size());
    double expected = 0d;
    for (Double val : Iterables.concat(vals1, vals2)) {
      expected += (val - meanTotal) * (val - meanTotal);
    }

    Assert.assertEquals(expected, NullableMath.m2(m21, mean1, vals1.size(), m22, mean2, vals2.size()), 0.0000001d);
  }
}
