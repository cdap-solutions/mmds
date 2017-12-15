package co.cask.mmds.stats;

import co.cask.mmds.data.HistogramBin;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Histogram utilities.
 */
public class Histograms {
  private static final DecimalFormat NOTATION_FORMAT = new DecimalFormat("0.00E0");
  private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("###.####");

  static {
    NOTATION_FORMAT.setRoundingMode(RoundingMode.HALF_UP);
  }

  private Histograms() {
    // private constructor so nobody can instantiate
  }

  public static List<HistogramBin> convert(NumericHisto numericHisto) {
    return convert(numericHisto, bin -> String.format(bin.isHiInclusive() ? "[%s,%s]" : "[%s,%s)",
                                                      format(bin.getLo()), format(bin.getHi())));
  }

  public static List<HistogramBin> convert(NumericHisto numericHisto, Function<NumericBin, String> formatter) {
    List<HistogramBin> bins = new ArrayList<>(numericHisto.getBins().size());
    for (NumericBin bin : numericHisto.getBins()) {
      String binStr = formatter.apply(bin);
      bins.add(new HistogramBin(binStr, bin.getCount()));
    }
    return bins;
  }

  public static List<HistogramBin> convert(CategoricalHisto histo) {
    List<HistogramBin> bins = new ArrayList<>(histo.getCounts().size());
    for (Map.Entry<String, Long> histoEntry : histo.getCounts().entrySet()) {
      bins.add(new HistogramBin(histoEntry.getKey(), histoEntry.getValue()));
    }
    // sort in descending order
    bins.sort((h1, h2) -> Long.compare(h2.getCount(), h1.getCount()));
    return bins;
  }

  private static String format(double val) {
    double mag = Math.abs(val);
    return (mag > 1000d || (mag < 0.001d && mag > 0d) ? NOTATION_FORMAT : DECIMAL_FORMAT).format(val);
  }
}
