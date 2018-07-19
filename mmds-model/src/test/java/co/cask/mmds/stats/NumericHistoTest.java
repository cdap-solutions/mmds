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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for numeric histograms.
 */
public class NumericHistoTest {

  @Test
  public void testOneBin() {
    NumericHisto histo = new NumericHisto(123d, 123d, 10, 123d);

    Assert.assertEquals(1, histo.getBins().size());
    Assert.assertEquals(123d, histo.getBins().get(0).getLo(), 0.0000001d);
    Assert.assertEquals(123d, histo.getBins().get(0).getHi(), 0.0000001d);
  }

  @Test
  public void testSimpleBinning() {
    NumericHisto histo = new NumericHisto(0d, 1d, 10, 1d);

    Assert.assertEquals(histo.getMin(), 1d, 0.0000001d);
    Assert.assertEquals(histo.getMax(), 1d, 0.0000001d);
    Assert.assertEquals(histo.getBins().size(), 10);
    for (int i = 0; i < 9; i++) {
      NumericBin bin = histo.getBins().get(i);
      Assert.assertEquals(0, bin.getCount());
    }
    Assert.assertEquals(1, histo.getBins().get(9).getCount());
  }

  @Test
  public void testSmallBinRounding() {
    NumericHisto histo = new NumericHisto(-0.00123456d, 0.0057983d, 10, 0.0003);

    Assert.assertEquals(histo.getBins().get(0).getLo(), -0.0012d, 0.0000001d);
    Assert.assertTrue(histo.getBins().get(9).getHi() >= 0.0057983d);
  }

  @Test
  public void testLargeBinRounding() {
    NumericHisto histo = new NumericHisto(1234567890000d, 9876543210000d, 10, 5555555555000d);

    Assert.assertEquals(histo.getBins().get(0).getLo(), 1200000000000d, 0.0000001d);
    Assert.assertTrue(histo.getBins().get(9).getHi() >= 9876543210000d);
  }

  @Test
  public void testStats() {
    NumericHisto histo1 = new NumericHisto(1d, 10d, 10, 1d);
    NumericHisto histo2 = new NumericHisto(1d, 10d, 10, 2d);
    NumericHisto histo3 = new NumericHisto(1d, 10d, 10, 3d);
    NumericHisto histo4 = new NumericHisto(1d, 10d, 10, 4d);

    NumericHisto histo1_2 = histo1.merge(histo2);
    NumericHisto histo3_4 = histo3.merge(histo4);

    NumericHisto histo = histo1_2.merge(histo3_4);

    Assert.assertEquals(2.5d, histo.getMean(), 0.0000001d);
    Assert.assertEquals(1.1180339887499d, histo.getStddev(), 0.0000001d);
  }

}
