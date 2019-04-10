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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * A thin layer on top of the underlying Table that stores the experiment data. Handles scanning, deletion,
 * serialization, deserialization, etc. This is not a custom dataset because custom datasets cannot currently
 * be used in plugins.
 */
public class ExperimentMetaTable extends CountTable<IndexedTable> {
  private static final Gson GSON = new Gson();
  private static final Type LIST_TYPE = new TypeToken<List<String>>() { }.getType();
  private static final String NAME_COL = "name";
  private static final String DESC_COL = "description";
  private static final String SRCPATH_COL = "srcpath";
  private static final String OUTCOME_COL = "outcome";
  private static final String OUTCOME_TYPE_COL = "outcomeType";
  private static final String DIRECTIVES_COL = "directives";
  public static final DatasetProperties DATASET_PROPERTIES = DatasetProperties.builder()
    .add(IndexedTable.INDEX_COLUMNS_CONF_KEY, Joiner.on(",").join(NAME_COL, SRCPATH_COL))
    .build();

  public ExperimentMetaTable(IndexedTable table) {
    super(table);
  }

  /**
   * List all experiments. Never returns null. If there are no experiments, returns an empty list.
   *
   * @param offset the number of initial experiments to ignore and not add to the results
   * @param limit upper limit on number of results returned.
   *
   * @return all experiments starting from offset
   */
  public ExperimentsMeta list(int offset, int limit) {
    return list(offset, limit, null, new SortInfo(SortType.ASC));
  }

  /**
   * List all experiments. Never returns null. If there are no experiments, returns an empty list.
   *
   * @param offset the number of initial experiments to ignore and not add to the results
   * @param limit upper limit on number of results returned.
   * @param predicate predicate to filter experiments
   * @param sortInfo sort information about sort order and field
   *
   * @return all experiments starting from offset with given source path
   */
  public ExperimentsMeta list(int offset, int limit, Predicate<Experiment> predicate, SortInfo sortInfo) {
    int count = 0;
    int cursor = 0;
    SortType sortType = sortInfo.getSortType();

    if (sortType.equals(SortType.DESC)) {
      offset = (int) (getTotalCount() - offset - limit);

      if (offset < 0) {
        limit = limit - Math.abs(offset);
        offset = 0;
      }
    }

    List<Experiment> experiments = new ArrayList<>();
    try (Scanner scanner = table.scanByIndex(Bytes.toBytes(sortInfo.getFields().get(0)), new byte[] { 0, 0 }, null)) {
      Row row;

      while ((row = scanner.next()) != null) {
        if (cursor < offset) {
          cursor++;
          continue;
        }

        if (count >= limit) {
          break;
        }

        Experiment e = fromRow(row);

        if (predicate == null || predicate.test(e)) {
          experiments.add(e);
          count++;
        }
      }
    }

    if (sortType.equals(SortType.DESC)) {
      Collections.sort(experiments, new Comparator<Experiment>() {
        @Override
        public int compare(Experiment o1, Experiment o2) {
          return o2.getName().compareTo(o1.getName());
        }
      });
    }

    return new ExperimentsMeta(getTotalCount(), experiments);
  }


  /**
   * Get information about the specified experiment.
   *
   * @param name the experiment name
   * @return information about the specified experiment
   */
  @Nullable
  public Experiment get(String name) {
    Row row = table.get(Bytes.toBytes(name));
    return row.isEmpty() ? null : fromRow(row);
  }

  /**
   * Delete the specified experiment
   *
   * @param name the experiment name
   */
  public void delete(String name) {
    table.delete(Bytes.toBytes(name));
    decrementRowCount(1);
  }

  /**
   * Add or update the specified experiment.
   *
   * @param experiment the experiment to write
   */
  public void put(Experiment experiment) {
    boolean isNewExperiment = get(experiment.getName()) == null;
    Put put = new Put(experiment.getName())
      .add(NAME_COL, experiment.getName())
      .add(DESC_COL, experiment.getDescription())
      .add(SRCPATH_COL, experiment.getSrcpath())
      .add(OUTCOME_COL, experiment.getOutcome())
      .add(OUTCOME_TYPE_COL, experiment.getOutcomeType())
      .add(DIRECTIVES_COL, GSON.toJson(experiment.getDirectives()));
    table.put(put);

    if (isNewExperiment) {
      incrementRowCount();
    }
  }

  private Experiment fromRow(Row row) {
    return new Experiment(row.getString(NAME_COL), row.getString(DESC_COL), row.getString(SRCPATH_COL),
                          row.getString(OUTCOME_COL), row.getString(OUTCOME_TYPE_COL),
                          GSON.fromJson(row.getString(DIRECTIVES_COL), LIST_TYPE));
  }
}
