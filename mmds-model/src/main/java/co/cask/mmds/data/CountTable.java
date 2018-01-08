package co.cask.mmds.data;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;

/**
 * Keeps count of total number of rows in a give table
 */
public class CountTable {
  protected final static byte[] TOTALS_ROW_KEY = new byte[] { 0 };
  protected final static String TOTALS_COL = "totals";
  protected final Table table;

  public CountTable(Table table) {
    this.table = table;
  }

  protected long getTotalCount() {
    byte[] total = table.get(TOTALS_ROW_KEY, Bytes.toBytes(TOTALS_COL));
    return total == null ? 0 : Bytes.toLong(total);
  }

  protected void incrementRowCount() {
    // increment row count
    table.increment(new Increment(TOTALS_ROW_KEY).add(TOTALS_COL, 1L));
  }

  protected void decrementRowCount(int rowCount) {
    long totalCount = getTotalCount();
    if (totalCount - rowCount < 0) {
      throw  new IllegalStateException("Cannot decrement row count below 0");
    }
    // decrement row count by rowCount
    table.put(new Put(TOTALS_ROW_KEY).add(TOTALS_COL,  totalCount - (long) rowCount));
  }
}
