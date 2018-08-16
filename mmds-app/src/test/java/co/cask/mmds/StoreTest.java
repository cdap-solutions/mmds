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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.IndexedTable;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestBaseWithSpark2;
import co.cask.cdap.test.TestConfiguration;
import co.cask.mmds.data.DataSplit;
import co.cask.mmds.data.DataSplitInfo;
import co.cask.mmds.data.DataSplitStats;
import co.cask.mmds.data.DataSplitTable;
import co.cask.mmds.data.EvaluationMetrics;
import co.cask.mmds.data.Experiment;
import co.cask.mmds.data.ExperimentMetaTable;
import co.cask.mmds.data.ExperimentStore;
import co.cask.mmds.data.ModelKey;
import co.cask.mmds.data.ModelMeta;
import co.cask.mmds.data.ModelStatus;
import co.cask.mmds.data.ModelTable;
import co.cask.mmds.data.SortInfo;
import co.cask.mmds.data.SortType;
import co.cask.mmds.data.SplitKey;
import co.cask.mmds.data.SplitStatus;
import co.cask.mmds.manager.ModelPrepApp;
import co.cask.mmds.proto.ConflictException;
import co.cask.mmds.proto.CreateModelRequest;
import co.cask.mmds.proto.TrainModelRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests.
 */
public class StoreTest extends TestBaseWithSpark2 {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  private DataSetManager<IndexedTable> experimentsManager;
  private ExperimentMetaTable experimentsTable;
  private DataSetManager<PartitionedFileSet> splitsManager;
  private DataSplitTable splitTable;
  private DataSetManager<IndexedTable> modelsManager;
  private ModelTable modelTable;
  private ExperimentStore store;

  @BeforeClass
  public static void setupTestClass() {
    deployApplication(ModelPrepApp.class);
  }

  @Before
  public void setupTest() throws Exception {
    experimentsManager = getDataset(Constants.Dataset.EXPERIMENTS_META);
    experimentsTable = new ExperimentMetaTable(experimentsManager.get());
    splitsManager = getDataset(Constants.Dataset.SPLITS);
    splitTable = new DataSplitTable(splitsManager.get());
    modelsManager = getDataset(Constants.Dataset.MODEL_META);
    modelTable = new ModelTable(modelsManager.get());
    store = new ExperimentStore(experimentsTable, splitTable, modelTable);
  }

  private void flush() {
    experimentsManager.flush();
    splitsManager.flush();
    modelsManager.flush();
  }

  @Test
  public void testExperimentsTable() {
    Assert.assertTrue(experimentsTable.list(0, 50).getExperiments().isEmpty());

    String experiment1Name = "exp123";
    Assert.assertNull(experimentsTable.get(experiment1Name));

    Experiment experiment1 = new Experiment(experiment1Name, "desc", "src", "outcome", "string",
                                            Collections.emptyList());
    experimentsTable.put(experiment1);

    Assert.assertEquals(experiment1, experimentsTable.get(experiment1Name));
    Assert.assertEquals(ImmutableList.of(experiment1), experimentsTable.list(0, 2).getExperiments());

    String experiment2Name = "exp456";
    Experiment experiment2 = new Experiment(experiment2Name, "d", "s", "o", "string", Collections.emptyList());
    experimentsTable.put(experiment2);

    Assert.assertEquals(experiment2, experimentsTable.get(experiment2Name));
    Assert.assertEquals(experimentsTable.list(0, 50).getTotalRowCount(), 2L);

    List<Experiment> list = experimentsTable.list(0, 1).getExperiments();
    Assert.assertEquals(ImmutableList.of(experiment1), list);
    list = experimentsTable.list(1, 1).getExperiments();
    Assert.assertEquals(ImmutableList.of(experiment2), list);

    list = experimentsTable.list(0, 1, e -> e.getSrcpath().equals("src"), new SortInfo(SortType.ASC)).getExperiments();
    Assert.assertEquals(ImmutableList.of(experiment1), list);

    experimentsTable.delete(experiment2Name);
    flush();
    Assert.assertNull(experimentsTable.get(experiment2Name));
    Assert.assertEquals(experimentsTable.list(0, 50).getTotalRowCount(), 1L);

    experimentsTable.delete(experiment1Name);
    flush();
    Assert.assertNull(experimentsTable.get(experiment1Name));

    Assert.assertTrue(experimentsTable.list(0, 50).getExperiments().isEmpty());
    Assert.assertEquals(experimentsTable.list(0, 50).getTotalRowCount(), 0L);
  }

  @Test
  public void testModelsTable() {
    Experiment experiment1 = new Experiment("e1", "", "path", "o1", "string", Collections.emptyList());
    Experiment experiment2 = new Experiment("e2", "", "path", "o1", "string", Collections.emptyList());

    Assert.assertNull(modelTable.get(new ModelKey(experiment1.getName(), "abc")));
    Assert.assertTrue(modelTable.list(experiment1.getName(), 0, 10, new SortInfo(SortType.ASC)).getModels().isEmpty());
    Assert.assertTrue(modelTable.list(experiment2.getName(), 0, 10, new SortInfo(SortType.ASC)).getModels().isEmpty());

    ModelMeta e1Model1 = build(experiment1, "model1");
    ModelMeta e1Model2 = build(experiment1, "model2");
    ModelMeta e1Model3 = build(experiment1, "model3");
    ModelMeta e1Model4 = build(experiment1, "model4");
    ModelMeta e1Model5 = build(experiment1, "model5");

    Assert.assertEquals(ImmutableList.of(e1Model1, e1Model2), modelTable.list(experiment1.getName(), 0, 2,
                                                                      new SortInfo(SortType.ASC)).getModels());
    Assert.assertEquals(ImmutableList.of(e1Model3, e1Model4), modelTable.list(experiment1.getName(), 2, 2,
                                                                              new SortInfo(SortType.ASC)).getModels());
    Assert.assertEquals(ImmutableList.of(e1Model5), modelTable.list(experiment1.getName(), 4, 10,
                                                                              new SortInfo(SortType.ASC)).getModels());

    ModelMeta e2Model1 = build(experiment2, "model1");
    ModelMeta e2Model2 = build(experiment2, "model2");
    ModelMeta e2Model3 = build(experiment2, "model3");
    ModelMeta e2Model4 = build(experiment2, "model4");
    ModelMeta e2Model5 = build(experiment2, "model5");

    Assert.assertEquals(ImmutableList.of(e2Model5, e2Model4), modelTable.list(experiment2.getName(), 0, 2,
                                                                              new SortInfo(SortType.DESC)).getModels());
    Assert.assertEquals(ImmutableList.of(e2Model3, e2Model2), modelTable.list(experiment2.getName(), 2, 2,
                                                                              new SortInfo(SortType.DESC)).getModels());
    Assert.assertEquals(ImmutableList.of(e2Model1), modelTable.list(experiment2.getName(), 4, 10,
                                                                    new SortInfo(SortType.DESC)).getModels());

    modelTable.delete(experiment1.getName());
    flush();
    Assert.assertTrue(modelTable.list(experiment1.getName(), 0, 10, new SortInfo(SortType.ASC)).getModels().isEmpty());
    Assert.assertNull(modelTable.get(new ModelKey(experiment1.getName(), e1Model1.getId())));
    Assert.assertFalse(modelTable.list(experiment2.getName(), 0, 10, new SortInfo(SortType.ASC)).getModels().isEmpty());
    modelTable.delete(experiment2.getName());
    flush();
    Assert.assertTrue(modelTable.list(experiment2.getName(), 0, 10, new SortInfo(SortType.ASC)).getModels().isEmpty());
    Assert.assertNull(modelTable.get(new ModelKey(experiment2.getName(), e1Model2.getId())));
  }

  @Test
  public void testSplitsTable() {
    String experiment1 = "e1";
    String experiment2 = "e2";
    Assert.assertTrue(splitTable.list(experiment1).isEmpty());
    Assert.assertTrue(splitTable.list(experiment2).isEmpty());
    Assert.assertNull(splitTable.get(new SplitKey(experiment1, "s123")));
    Assert.assertNull(splitTable.get(new SplitKey(experiment2, "s456")));

    DataSplit split1 = DataSplit.builder()
      .setDescription("desc")
      .setDirectives(ImmutableList.of("d1", "d2"))
      .setParams(ImmutableMap.of("p1", "v1"))
      .setSchema(Schema.recordOf("s1", Schema.Field.of("f1", Schema.of(Schema.Type.STRING))))
      .setType("random")
      .build();
    String split1Id = splitTable.addSplit(experiment1, split1, 100);
    SplitKey split1Key = new SplitKey(experiment1, split1Id);
    DataSplitStats split1Stats = DataSplitStats.builder(split1Id)
      .setStartTime(100L)
      .setDescription(split1.getDescription())
      .setDirectives(split1.getDirectives())
      .setParams(split1.getParams())
      .setSchema(split1.getSchema())
      .setType(split1.getType())
      .setStatus(SplitStatus.SPLITTING)
      .build();

    DataSplit split2 = DataSplit.builder()
      .setDescription("desc")
      .setDirectives(ImmutableList.of("d3"))
      .setParams(ImmutableMap.of("p2", "v2"))
      .setSchema(Schema.recordOf("s1", Schema.Field.of("f1", Schema.of(Schema.Type.STRING))))
      .setType("random")
      .build();
    String split2Id = splitTable.addSplit(experiment2, split2, 200);
    SplitKey split2Key = new SplitKey(experiment2, split2Id);
    DataSplitStats split2Stats = DataSplitStats.builder(split2Id)
      .setStartTime(200L)
      .setDescription(split2.getDescription())
      .setDirectives(split2.getDirectives())
      .setParams(split2.getParams())
      .setSchema(split2.getSchema())
      .setType(split2.getType())
      .setStatus(SplitStatus.SPLITTING)
      .build();

    Assert.assertEquals(split1Stats, splitTable.get(split1Key));
    Assert.assertEquals(split2Stats, splitTable.get(split2Key));
    Assert.assertEquals(ImmutableList.of(split1Stats), splitTable.list(experiment1));
    Assert.assertEquals(ImmutableList.of(split2Stats), splitTable.list(experiment2));

    splitTable.delete(split1Key);
    flush();
    Assert.assertTrue(splitTable.list(experiment1).isEmpty());
    Assert.assertFalse(splitTable.list(experiment2).isEmpty());
    Assert.assertNull(splitTable.get(split1Key));

    splitTable.delete(split2Key);
    flush();
    Assert.assertTrue(splitTable.list(experiment2).isEmpty());
    Assert.assertNull(splitTable.get(split2Key));
  }

  @Test
  public void testExperimentsSorting() {
    Assert.assertTrue(experimentsTable.list(0, 50).getExperiments().isEmpty());

    String experiment1Name = "abc123";
    Assert.assertNull(experimentsTable.get(experiment1Name));

    Experiment experiment1 = new Experiment(experiment1Name, "desc", "src", "outcome", "string",
                                            Collections.emptyList());
    experimentsTable.put(experiment1);

    String experiment2Name = "abd345";
    Experiment experiment2 = new Experiment(experiment2Name, "d", "s", "o", "string", Collections.emptyList());
    experimentsTable.put(experiment2);

    String experiment3Name = "bda345";
    Experiment experiment3 = new Experiment(experiment3Name, "d", "s", "o", "string", Collections.emptyList());
    experimentsTable.put(experiment3);

    String experiment4Name = "zsd345";
    Experiment experiment4 = new Experiment(experiment4Name, "d", "s", "o", "string", Collections.emptyList());
    experimentsTable.put(experiment4);

    String experiment5Name = "yea345";
    Experiment experiment5 = new Experiment(experiment5Name, "d", "s", "o", "string", Collections.emptyList());
    experimentsTable.put(experiment5);

    Assert.assertEquals(experimentsTable.list(0, 50).getTotalRowCount(), 5L);

    List<Experiment> list = experimentsTable.list(0, 20).getExperiments();
    Assert.assertEquals(ImmutableList.of(experiment1, experiment2, experiment3, experiment5, experiment4), list);

    list = experimentsTable.list(0, 20, null, new SortInfo(SortType.DESC)).getExperiments();
    Assert.assertEquals(ImmutableList.of(experiment4, experiment5, experiment3, experiment2, experiment1), list);

    list = experimentsTable.list(0, 2, null, new SortInfo(SortType.DESC)).getExperiments();
    Assert.assertEquals(ImmutableList.of(experiment4, experiment5), list);

    list = experimentsTable.list(2, 2, null, new SortInfo(SortType.DESC)).getExperiments();
    Assert.assertEquals(ImmutableList.of(experiment3, experiment2), list);

    list = experimentsTable.list(4, 2, null, new SortInfo(SortType.DESC)).getExperiments();
    Assert.assertEquals(ImmutableList.of(experiment1), list);

    experimentsTable.delete(experiment5Name);
    flush();

    experimentsTable.delete(experiment4Name);
    flush();

    experimentsTable.delete(experiment3Name);
    flush();

    experimentsTable.delete(experiment2Name);
    flush();

    experimentsTable.delete(experiment1Name);
    flush();

    Assert.assertTrue(experimentsTable.list(0, 50).getExperiments().isEmpty());
    Assert.assertEquals(experimentsTable.list(0, 50).getTotalRowCount(), 0L);
  }

  @Test
  public void testModelLifecycle() {
    // create an experiment
    Experiment experiment = new Experiment("re", "desc", "srcpath", "outcome", "string", Collections.emptyList());
    store.putExperiment(experiment);
    flush();

    // create a model
    List<String> directives = new ArrayList<>();
    directives.add("d1");
    directives.add("d2");
    CreateModelRequest createModelRequest = new CreateModelRequest("model1", "desc", directives, null);
    String modelId = store.addModel(experiment.getName(), createModelRequest);
    flush();
    ModelKey modelKey = new ModelKey(experiment.getName(), modelId);

    ModelMeta modelMeta = store.getModel(modelKey);
    Assert.assertEquals(ModelStatus.PREPARING, modelMeta.getStatus());

    // update directives on the model
    directives.add("d3");
    store.setModelDirectives(modelKey, directives);
    flush();

    modelMeta = store.getModel(modelKey);
    Assert.assertEquals(directives, modelMeta.getDirectives());

    TrainModelRequest trainModelRequest =
      new TrainModelRequest("decision.tree.classifier", null, Collections.emptyMap());

    // cannot unassign a split since there is no split
    try {
      store.unassignModelSplit(modelKey);
      Assert.fail();
    } catch (ConflictException e) {
      // expected
    }
    // cannot train the model without a split
    try {
      store.trainModel(modelKey, trainModelRequest, 100);
      Assert.fail();
    } catch (ConflictException e) {
      // expected
    }

    // create a split for the model
    Schema schema = Schema.recordOf("rec",
                                    Schema.Field.of("outcome", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("f1", Schema.of(Schema.Type.STRING)));
    DataSplit dataSplit = new DataSplit("desc", "random", Collections.emptyMap(), directives, schema);
    DataSplitInfo dataSplitInfo = store.addSplit(experiment.getName(), dataSplit, 100);
    flush();
    SplitKey splitKey = new SplitKey(experiment.getName(), dataSplitInfo.getSplitId());

    // assign the split to the model, model should be in SPLITTING state
    store.setModelSplit(modelKey, splitKey.getSplit());
    flush();
    modelMeta = store.getModel(modelKey);
    Assert.assertEquals(ModelStatus.SPLITTING, modelMeta.getStatus());
    // cannot unassign a split until it is in SPLIT_FAILED or DATA_READY state
    try {
      store.unassignModelSplit(modelKey);
      Assert.fail();
    } catch (ConflictException e) {
      // expected
    }
    // cannot train the model without a split
    try {
      store.trainModel(modelKey, trainModelRequest, 100);
      Assert.fail();
    } catch (ConflictException e) {
      // expected
    }

    // finish the split
    store.finishSplit(splitKey, "trainingPath", "testPath", Collections.emptyList(), System.currentTimeMillis());
    flush();

    // model should now be in DATA_READY state with features set
    modelMeta = store.getModel(modelKey);
    Assert.assertEquals(ModelStatus.DATA_READY, modelMeta.getStatus());
    Assert.assertEquals(ImmutableList.of("f1"), modelMeta.getFeatures());

    // should now be able to unassign the split
    store.unassignModelSplit(modelKey);
    flush();
    // split should have been deleted too
    Assert.assertTrue(store.listSplits(experiment.getName()).isEmpty());

    // model should now be back in PREPARING state
    modelMeta = store.getModel(modelKey);
    Assert.assertEquals(ModelStatus.PREPARING, modelMeta.getStatus());

    // create another split and assign it to the model again, model should be in SPLITTING
    dataSplitInfo = store.addSplit(experiment.getName(), dataSplit, 100);
    flush();
    splitKey = new SplitKey(experiment.getName(), dataSplitInfo.getSplitId());
    store.setModelSplit(modelKey, splitKey.getSplit());
    flush();
    modelMeta = store.getModel(modelKey);
    Assert.assertEquals(ModelStatus.SPLITTING, modelMeta.getStatus());

    // finish the split
    store.finishSplit(splitKey, "trainingPath", "testPath", Collections.emptyList(), System.currentTimeMillis());
    flush();
    // model should now be in DATA_READY state with features set
    modelMeta = store.getModel(modelKey);
    Assert.assertEquals(ModelStatus.DATA_READY, modelMeta.getStatus());
    Assert.assertEquals(ImmutableList.of("f1"), modelMeta.getFeatures());

    // now train the model
    store.trainModel(modelKey, trainModelRequest, 100);
    flush();
    // model should be in TRAINING state
    modelMeta = store.getModel(modelKey);
    Assert.assertEquals(ModelStatus.TRAINING, modelMeta.getStatus());

    // should not be able to perform any lifecycle operations
    try {
      store.trainModel(modelKey, trainModelRequest, 100);
      Assert.fail();
    } catch (ConflictException e) {
      // expected
    }
    try {
      store.unassignModelSplit(modelKey);
      Assert.fail();
    } catch (ConflictException e) {
      // expected
    }

    store.updateModelMetrics(modelKey, new EvaluationMetrics(1.0f, 1.0f, 1.0f), System.currentTimeMillis(),
                             ImmutableSet.of("f1"));
    flush();

    // should now be in TRAINED state
    modelMeta = store.getModel(modelKey);
    Assert.assertEquals(ModelStatus.TRAINED, modelMeta.getStatus());

    // creating a new model with an existing split should create it in DATA_READY state.
    createModelRequest = new CreateModelRequest("model2", "desc", Collections.emptyList(), splitKey.getSplit());
    String modelId2 = store.addModel(experiment.getName(), createModelRequest);
    flush();

    // check model is in DATA_READY with the correct directives and features set
    ModelKey modelKey2 = new ModelKey(experiment.getName(), modelId2);
    modelMeta = store.getModel(modelKey2);
    Assert.assertEquals(ModelStatus.DATA_READY, modelMeta.getStatus());
    Assert.assertEquals(ImmutableList.of("f1"), modelMeta.getFeatures());
    Assert.assertEquals(directives, modelMeta.getDirectives());
  }

  private ModelMeta build(Experiment experiment, String model) {
    long createTs = System.currentTimeMillis();
    CreateModelRequest createRequest = new CreateModelRequest(model, "desc1", Collections.emptyList(), null);
    String model1Id = modelTable.add(experiment, createRequest, createTs);
    return ModelMeta.builder(model1Id)
      .setDescription(createRequest.getDescription())
      .setName(createRequest.getName())
      .setOutcome(experiment.getOutcome())
      .setCreateTime(createTs)
      .setStatus(ModelStatus.PREPARING)
      .setEvaluationMetrics(new EvaluationMetrics(null, null, null, null, null, null, null))
      .build();
  }
}
