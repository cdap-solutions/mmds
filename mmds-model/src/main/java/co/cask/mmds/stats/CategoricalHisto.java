package co.cask.mmds.stats;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Histogram for a categorical column.
 */
public class CategoricalHisto extends Histogram<CategoricalHisto> implements Serializable {
  private static final long serialVersionUID = 5788076293831975440L;
  private final Map<String, Long> counts;
  private long emptyCount;

  public CategoricalHisto() {
    this(0L, 0L, 0L, new HashMap<>());
  }

  public CategoricalHisto(long totalCount, long nullCount, long emptyCount, Map<String, Long> counts) {
    super(totalCount, nullCount);
    this.counts = new HashMap<>(counts);
    this.emptyCount = emptyCount;
  }

  public long getEmptyCount() {
    return emptyCount;
  }

  public Map<String, Long> getCounts() {
    return counts;
  }

  public void update(String val) {
    Long currentVal = counts.get(val);
    if (currentVal == null) {
      counts.put(val, 1L);
    } else {
      counts.put(val, currentVal + 1);
    }
    totalCount++;
    if (val == null) {
      nullCount++;
    } else if (val.isEmpty()) {
      emptyCount++;
    }
  }

  @Override
  public CategoricalHisto merge(CategoricalHisto other) {
    for (Map.Entry<String, Long> entry : other.counts.entrySet()) {
      String key = entry.getKey();
      Long count = entry.getValue();
      Long existing = counts.get(key);
      counts.put(key, existing == null ? count : count + existing);
    }
    return new CategoricalHisto(totalCount + other.totalCount, nullCount + other.nullCount,
                                emptyCount + other.emptyCount, counts);
  }
}
