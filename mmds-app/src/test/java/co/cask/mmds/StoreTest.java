/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestBaseWithSpark2;
import co.cask.cdap.test.TestConfiguration;
import co.cask.mmds.data.DataSplit;
import co.cask.mmds.data.DataSplitStats;
import co.cask.mmds.data.DataSplitTable;
import co.cask.mmds.data.EvaluationMetrics;
import co.cask.mmds.data.Experiment;
import co.cask.mmds.data.ExperimentMetaTable;
import co.cask.mmds.data.Model;
import co.cask.mmds.data.ModelKey;
import co.cask.mmds.data.ModelMeta;
import co.cask.mmds.data.ModelStatus;
import co.cask.mmds.data.ModelTable;
import co.cask.mmds.data.SplitKey;
import co.cask.mmds.data.SplitStatus;
import co.cask.mmds.manager.ModelPrepApp;
import co.cask.mmds.proto.CreateModelRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Set;

/**
 * Unit tests.
 */
public class StoreTest extends TestBaseWithSpark2 {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @BeforeClass
  public static void setupTestClass() throws Exception {
    deployApplication(ModelPrepApp.class);
  }

  @Test
  public void testExperimentsTable() throws Exception {
    DataSetManager<Table> manager = getDataset(Constants.Dataset.EXPERIMENTS_META);
    ExperimentMetaTable experimentsTable = new ExperimentMetaTable(manager.get());

    Assert.assertTrue(experimentsTable.list().isEmpty());

    String experiment1Name = "exp123";
    Assert.assertNull(experimentsTable.get(experiment1Name));

    Experiment experiment1 = new Experiment(experiment1Name, "desc", "src", "outcome", "string", "work1");
    experimentsTable.put(experiment1);

    Assert.assertEquals(experiment1, experimentsTable.get(experiment1Name));
    Assert.assertEquals(ImmutableList.of(experiment1), experimentsTable.list());

    String experiment2Name = "exp456";
    Experiment experiment2 = new Experiment(experiment2Name, "d", "s", "o", "string", "work2");
    experimentsTable.put(experiment2);

    Assert.assertEquals(experiment2, experimentsTable.get(experiment2Name));
    Assert.assertEquals(ImmutableList.of(experiment1, experiment2), experimentsTable.list());

    experimentsTable.delete(experiment2Name);
    manager.flush();
    Assert.assertNull(experimentsTable.get(experiment2Name));

    experimentsTable.delete(experiment1Name);
    manager.flush();
    Assert.assertNull(experimentsTable.get(experiment1Name));

    Assert.assertTrue(experimentsTable.list().isEmpty());
  }

  @Test
  public void testModelsTable() throws Exception {
    DataSetManager<Table> manager = getDataset(Constants.Dataset.MODEL_META);
    ModelTable modelTable = new ModelTable(manager.get());

    Experiment experiment1 = new Experiment("e1", "", "path", "o1", "string", "workspace");
    Experiment experiment2 = new Experiment("e2", "", "path", "o1", "string", "workspace");

    Assert.assertNull(modelTable.get(new ModelKey(experiment1.getName(), "abc")));
    Assert.assertTrue(modelTable.list(experiment1.getName()).isEmpty());
    Assert.assertTrue(modelTable.list(experiment2.getName()).isEmpty());

    long createTs = System.currentTimeMillis();
    CreateModelRequest createRequest = new CreateModelRequest("model1", "desc1");
    String model1Id = modelTable.add(experiment1, createRequest, createTs);
    ModelMeta model1Meta = ModelMeta.builder(model1Id)
      .setDescription(createRequest.getDescription())
      .setName(createRequest.getName())
      .setOutcome(experiment1.getOutcome())
      .setCreateTime(createTs)
      .setStatus(ModelStatus.EMPTY)
      .setEvaluationMetrics(new EvaluationMetrics(null, null, null, null, null, null, null))
      .build();

    createRequest = new CreateModelRequest("model2", "desc2");
    String model2Id = modelTable.add(experiment2, createRequest, createTs);
    ModelMeta model2Meta = ModelMeta.builder(model2Id)
      .setDescription(createRequest.getDescription())
      .setName(createRequest.getName())
      .setOutcome(experiment2.getOutcome())
      .setCreateTime(createTs)
      .setStatus(ModelStatus.EMPTY)
      .setEvaluationMetrics(new EvaluationMetrics(null, null, null, null, null, null, null))
      .build();

    Assert.assertEquals(ImmutableList.of(model1Meta), modelTable.list(experiment1.getName()));
    Assert.assertEquals(model1Meta, modelTable.get(new ModelKey(experiment1.getName(), model1Id)));
    Assert.assertEquals(ImmutableList.of(model2Meta), modelTable.list(experiment2.getName()));
    Assert.assertEquals(model2Meta, modelTable.get(new ModelKey(experiment2.getName(), model2Id)));

    long trainTs = System.currentTimeMillis();
    List<String> features = ImmutableList.of("f1", "f2");
    Set<String> categoricalFeatures = ImmutableSet.of("f1");
    EvaluationMetrics evaluationMetrics = new EvaluationMetrics(.9d, .8d, .1d);
    modelTable.update(new ModelKey(experiment1.getName(), model1Id), evaluationMetrics,
                      trainTs, features, categoricalFeatures);
    model1Meta = ModelMeta.builder(model1Id)
      .setDescription("desc1")
      .setName("model1")
      .setCreateTime(createTs)
      .setTrainedTime(trainTs)
      .setFeatures(features)
      .setCategoricalFeatures(categoricalFeatures)
      .setEvaluationMetrics(evaluationMetrics)
      .setDeployTime(-1L)
      .setOutcome(experiment1.getOutcome())
      .setStatus(ModelStatus.TRAINED)
      .build();
    Assert.assertEquals(model1Meta, modelTable.get(new ModelKey(experiment1.getName(), model1Id)));

    modelTable.delete(experiment1.getName());
    manager.flush();
    Assert.assertTrue(modelTable.list(experiment1.getName()).isEmpty());
    Assert.assertNull(modelTable.get(new ModelKey(experiment1.getName(), model1Id)));
    Assert.assertFalse(modelTable.list(experiment2.getName()).isEmpty());
    modelTable.delete(experiment2.getName());
    manager.flush();
    Assert.assertTrue(modelTable.list(experiment2.getName()).isEmpty());
    Assert.assertNull(modelTable.get(new ModelKey(experiment2.getName(), model2Id)));
  }

  @Test
  public void testSplitsTable() throws Exception {
    DataSetManager<PartitionedFileSet> manager = getDataset(Constants.Dataset.SPLITS);
    DataSplitTable splitTable = new DataSplitTable(manager.get());

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
    String split1Id = splitTable.addSplit(experiment1, split1);
    SplitKey split1Key = new SplitKey(experiment1, split1Id);
    DataSplitStats split1Stats = DataSplitStats.builder(split1Id)
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
      .setType("firstN")
      .build();
    String split2Id = splitTable.addSplit(experiment2, split2);
    SplitKey split2Key = new SplitKey(experiment2, split2Id);
    DataSplitStats split2Stats = DataSplitStats.builder(split2Id)
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
    manager.flush();
    Assert.assertTrue(splitTable.list(experiment1).isEmpty());
    Assert.assertFalse(splitTable.list(experiment2).isEmpty());
    Assert.assertNull(splitTable.get(split1Key));

    splitTable.delete(split2Key);
    manager.flush();
    Assert.assertTrue(splitTable.list(experiment2).isEmpty());
    Assert.assertNull(splitTable.get(split2Key));
  }
}
