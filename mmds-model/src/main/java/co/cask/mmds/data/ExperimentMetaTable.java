package co.cask.mmds.data;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Joiner;

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
  private final static String NAME_COL = "name";
  private final static String DESC_COL = "description";
  private final static String SRCPATH_COL = "srcpath";
  private final static String OUTCOME_COL = "outcome";
  private final static String OUTCOME_TYPE_COL = "outcomeType";

  private final static Schema SCHEMA = Schema.recordOf(
    "experiments",
    Schema.Field.of(NAME_COL, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(DESC_COL, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(SRCPATH_COL, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(OUTCOME_COL, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(OUTCOME_TYPE_COL, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(TOTALS_COL, Schema.nullableOf(Schema.of(Schema.Type.LONG)))
  );
  public static final DatasetProperties DATASET_PROPERTIES = DatasetProperties.builder()
    .add(IndexedTable.INDEX_COLUMNS_CONF_KEY, Joiner.on(",").join(NAME_COL, SRCPATH_COL))
    .add(Table.PROPERTY_SCHEMA, SCHEMA.toString())
    .add(Table.PROPERTY_SCHEMA_ROW_FIELD, NAME_COL)
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
      .add(OUTCOME_TYPE_COL, experiment.getOutcomeType());
    table.put(put);

    if (isNewExperiment) {
      incrementRowCount();
    }
  }

  private Experiment fromRow(Row row) {
    return new Experiment(row.getString(NAME_COL), row.getString(DESC_COL), row.getString(SRCPATH_COL),
                          row.getString(OUTCOME_COL), row.getString(OUTCOME_TYPE_COL));
  }
}
