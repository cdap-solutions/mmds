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

}
