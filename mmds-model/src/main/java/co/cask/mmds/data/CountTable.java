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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.table.Increment;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;

/**
 * Keeps count of total number of rows in a give table
 */
public class CountTable<T extends Table> {
  protected final static byte[] TOTALS_ROW_KEY = new byte[] { 0 };
  protected final static String TOTALS_COL = "totals";
  protected final T table;

  public CountTable(T table) {
    this.table = table;
  }

  protected long getTotalCount() {
    return getTotalCount(TOTALS_COL);
  }

  protected long getTotalCount(String col) {
    byte[] total = table.get(TOTALS_ROW_KEY, Bytes.toBytes(col));
    return total == null ? 0 : Bytes.toLong(total);
  }

  protected void incrementRowCount() {
    incrementRowCount(TOTALS_COL);
  }

  protected void incrementRowCount(String col) {
    // increment row count
    table.increment(new Increment(TOTALS_ROW_KEY).add(col, 1L));
  }

  protected void decrementRowCount(int rowCount) {
    decrementRowCount(rowCount, TOTALS_COL);
  }

  protected void decrementRowCount(int rowCount, String col) {
    long totalCount = getTotalCount(col);

    if (totalCount - rowCount < 0) {
      throw  new IllegalStateException("Cannot decrement row count below 0");
    }
    // decrement row count by rowCount
    table.put(new Put(TOTALS_ROW_KEY).add(col,  totalCount - (long) rowCount));
  }
}
