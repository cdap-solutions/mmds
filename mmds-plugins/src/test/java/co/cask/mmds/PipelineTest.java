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

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.SparkCompute;
import co.cask.cdap.etl.api.batch.SparkSink;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import co.cask.mmds.data.ColumnStats;
import co.cask.mmds.data.DataSplit;
import co.cask.mmds.data.DataSplitStats;
import co.cask.mmds.data.DataSplitTable;
import co.cask.mmds.data.Experiment;
import co.cask.mmds.data.ExperimentStats;
import co.cask.mmds.data.HistogramBin;
import co.cask.mmds.data.ModelMeta;
import co.cask.mmds.data.ModelStatus;
import co.cask.mmds.data.SplitKey;
import co.cask.mmds.manager.Id;
import co.cask.mmds.manager.ModelManagerService;
import co.cask.mmds.manager.ModelPrepApp;
import co.cask.mmds.plugin.MLPredictor;
import co.cask.mmds.proto.CreateModelRequest;
import co.cask.mmds.proto.TrainModelRequest;
import co.cask.mmds.stats.CategoricalHisto;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Unit tests for our plugins.
 */
public class PipelineTest extends HydratorTestBase {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false,
                                                                       "app.program.spark.compat", "spark2_2.11");
  private static SparkManager sparkManager;
  private static URL serviceURL;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact, MLPredictor.class, CategoricalHisto.class);

    ApplicationManager applicationManager = deployApplication(ModelPrepApp.class);
    sparkManager = applicationManager.getSparkManager(ModelManagerService.NAME);
    sparkManager.start();
    sparkManager.waitForRun(ProgramRunStatus.RUNNING, 1, TimeUnit.MINUTES);
    while (serviceURL == null) {
      serviceURL = sparkManager.getServiceURL();
      TimeUnit.SECONDS.sleep(1);
    }
  }

  @AfterClass
  public static void cleanupTestClass() throws Exception {
    sparkManager.stop();
    sparkManager.waitForRun(ProgramRunStatus.KILLED, 1, TimeUnit.MINUTES);
  }

  @After
  public void cleanupTest() throws IOException {
    for (ExperimentStats experiment : listExperiments()) {
      deleteExperiment(experiment.getName());
    }
  }

  @Test
  public void testClassifier() throws Exception {
    String srcPath = getClass().getClassLoader().getResource("HR.csv").getPath();
    Experiment experiment = new Experiment("HR", "employee turnover", srcPath, "left",
                                           Schema.Type.BOOLEAN.name(), "workspace0");
    putExperiment(experiment);

    Schema schema = Schema.recordOf(
      "employeeLabels",
      Schema.Field.of("satisfaction", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("evaluation", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("projects", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("monthly_hours", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("time_at_company", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("accident", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("left", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("recent_promotion", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))),
      Schema.Field.of("department", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("salary", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    List<String> directives = ImmutableList.of(
      "parse-as-csv :body ',' false",
      "drop body",
      "rename body_1 satisfaction",
      "rename body_2 evaluation",
      "rename body_3 projects",
      "rename body_4 monthly_hours",
      "rename body_5 time_at_company",
      "rename body_6 accident",
      "rename body_7 left",
      "rename body_8 recent_promotion",
      "rename body_9 department",
      "rename body_10 salary",
      "set-type :satisfaction double",
      "set-type :evaluation double",
      "set-type :projects int",
      "set-type :monthly_hours int",
      "set-type :time_at_company int",
      "set-type :accident boolean",
      "set-type :left boolean",
      "set-type :recent_promotion boolean");
    DataSplit dataSplit = DataSplit.builder()
      .setType("random")
      .setDescription("random split")
      .setSchema(schema)
      .setDirectives(directives)
      .build();

    DataSplitStats splitStats = splitWithPipeline(experiment.getName(), dataSplit, inputManager -> {
      try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(getClass().getClassLoader().getResourceAsStream("HR.csv"), StandardCharsets.UTF_8))) {
        List<StructuredRecord> inputRecords = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
          String[] fields = line.split(",", 10);
          Double satisfaction = fields[0].isEmpty() ? null : Double.parseDouble(fields[0]);
          Double evaluation = fields[1].isEmpty() ? null : Double.parseDouble(fields[1]);
          Integer projects = fields[2].isEmpty() ? null : Integer.parseInt(fields[2]);
          Integer monthlyHours = fields[3].isEmpty() ? null : Integer.parseInt(fields[3]);
          Integer timeAtCompany = fields[4].isEmpty() ? null : Integer.parseInt(fields[4]);
          Boolean accident = fields[5].isEmpty() ? null : "1".equals(fields[5]);
          Boolean left = fields[6].isEmpty() ? null : "1".equals(fields[6]);
          Boolean recentPromotion = fields[7].isEmpty() ? null : "1".equals(fields[7]);
          String department = fields[8].isEmpty() ? null : fields[8];
          String salary = fields[9].isEmpty() ? null : fields[9];
          inputRecords.add(StructuredRecord.builder(schema)
                             .set("satisfaction", satisfaction)
                             .set("evaluation", evaluation)
                             .set("projects", projects)
                             .set("monthly_hours", monthlyHours)
                             .set("time_at_company", timeAtCompany)
                             .set("accident", accident)
                             .set("left", left)
                             .set("recent_promotion", recentPromotion)
                             .set("department", department)
                             .set("salary", salary).build());
          if (inputRecords.size() == 100) {
            MockSource.writeInput(inputManager, inputRecords);
            inputRecords.clear();
          }
        }
        MockSource.writeInput(inputManager, inputRecords);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    CreateModelRequest createModelRequest = new CreateModelRequest("dtree", "decision tree classifier");
    String modelId = createModel(experiment.getName(), createModelRequest);

    assignSplit(experiment.getName(), modelId, splitStats.getId());

    TrainModelRequest trainRequest = new TrainModelRequest("decision.tree.classifier", null, new HashMap<>());
    ModelMeta meta = trainModel(experiment.getName(), modelId, trainRequest, 240);

    Assert.assertNotNull(meta.getEvaluationMetrics().getPrecision());
    Assert.assertNotNull(meta.getEvaluationMetrics().getRecall());
    Assert.assertNotNull(meta.getEvaluationMetrics().getF1());

    // now use the model to make some predictions
    List<Schema.Field> predictionFields = new ArrayList<>(schema.getFields());
    predictionFields.add(Schema.Field.of("prediction", Schema.of(Schema.Type.STRING)));
    Schema predictionSchema = Schema.recordOf("prediction", predictionFields);
    String testInputTable = "classifierTestInput";
    String outputTable = "classifierOutput";
    ETLPlugin predictor = new ETLPlugin("MLPredictor", SparkCompute.PLUGIN_TYPE,
                                        ImmutableMap.of("experimentId", experiment.getName(),
                                                        "modelId", meta.getId(),
                                                        "predictionField", "prediction",
                                                        "schema", predictionSchema.toString()));
    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(testInputTable, schema)))
      .addStage(new ETLStage("predictor", predictor))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputTable)))
      .addConnection("source", "predictor")
      .addConnection("predictor", "sink")
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("classifierPredictor");
    ApplicationManager appManager = deployApplication(appId, new AppRequest<>(APP_ARTIFACT, config));

    // write some data to predict
    StructuredRecord employee = StructuredRecord.builder(schema)
      .set("satisfaction", 0.9d)
      .set("evaluation", 0.58d)
      .set("projects", 3)
      .set("monthly_hours", 150)
      .set("time_at_company", 5)
      .set("accident", false)
      .set("recent_promotion", false)
      .set("department", "sales")
      .set("salary", "low")
      .build();
    DataSetManager<Table> testInputManager = getDataset(testInputTable);
    MockSource.writeInput(testInputManager, ImmutableList.of(employee));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // check output
    DataSetManager<Table> outputManager = getDataset(outputTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Assert.assertEquals(1, outputRecords.size());
    String prediction = outputRecords.get(0).get("prediction");
    Assert.assertTrue("true".equals(prediction) || "false".equals(prediction));
  }

  @Test
  public void testRegressor() throws Exception {
    String srcPath = getClass().getClassLoader().getResource("sales.txt").getPath();
    Experiment experiment = new Experiment("re", "real estate", srcPath, "price",
                                           Schema.Type.DOUBLE.name(), "workspace1");
    putExperiment(experiment);

    Schema schema = Schema.recordOf(
      "sales",
      Schema.Field.of("price", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("city", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("zip", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("type", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("beds", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("baths", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("size", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("lot", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("stories", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("builtin", Schema.nullableOf(Schema.of(Schema.Type.INT))));
    List<String> directives = ImmutableList.of(
      "parse-as-csv :body '\\\\t' false",
      "drop body",
      "rename body_1 price",
      "rename body_2 city",
      "rename body_3 zip",
      "rename body_4 type",
      "rename body_5 beds",
      "rename body_6 baths",
      "rename body_7 size",
      "rename body_8 lot",
      "rename body_9 stories",
      "rename body_10 builtin",
      "set-type :price double",
      "fill-null-or-empty :builtin '-1'",
      "set-type :builtin int",
      "fill-null-or-empty :size 0",
      "set-type :size double",
      "fill-null-or-empty :lot 0",
      "set-type :lot double");
    DataSplit dataSplit = DataSplit.builder()
      .setType("random")
      .setDescription("random split")
      .setSchema(schema)
      .setDirectives(directives)
      .build();

    // can't do it this way until data splitting through spark service is done
    //DataSplitStats splitStats = addSplit(experiment.getName(), dataSplit, 180);

    // in the meantime, run a pipeline to do the splitting and modify state directly
    DataSplitStats splitStats = splitWithPipeline(experiment.getName(), dataSplit, inputManager -> {
      try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(getClass().getClassLoader().getResourceAsStream("sales.txt"), StandardCharsets.UTF_8))) {
        List<StructuredRecord> inputRecords = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
          String[] fields = line.split("\t", 13);
          Double price = fields[0].isEmpty() ? null : Double.parseDouble(fields[0]);
          String city = fields[1].isEmpty() ? null : fields[1];
          String zip = fields[2].isEmpty() ? null : fields[2];
          String type = fields[3].isEmpty() ? null : fields[3];
          String beds = fields[4].isEmpty() ? null : fields[4];
          String baths = fields[5].isEmpty() ? null : fields[5];
          Double size = fields[6].isEmpty() ? null : Double.parseDouble(fields[6]);
          Double lot = fields[7].isEmpty() ? null : Double.parseDouble(fields[7]);
          Double stories = fields[8].isEmpty() ? null : Double.parseDouble(fields[8]);
          Integer builtin = fields[9].isEmpty() ? null : Integer.parseInt(fields[9]);
          inputRecords.add(StructuredRecord.builder(schema)
                             .set("price", price)
                             .set("city", city)
                             .set("zip", zip)
                             .set("type", type)
                             .set("beds", beds)
                             .set("baths", baths)
                             .set("size", size)
                             .set("lot", lot)
                             .set("stories", stories)
                             .set("builtin", builtin).build());
          if (inputRecords.size() == 100) {
            MockSource.writeInput(inputManager, inputRecords);
            inputRecords.clear();
          }
        }
        MockSource.writeInput(inputManager, inputRecords);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    CreateModelRequest createModelRequest = new CreateModelRequest("dtree", "decision tree regression");
    String modelId = createModel(experiment.getName(), createModelRequest);

    assignSplit(experiment.getName(), modelId, splitStats.getId());

    TrainModelRequest trainRequest = new TrainModelRequest("decision.tree.regression", null, new HashMap<>());
    ModelMeta meta = trainModel(experiment.getName(), modelId, trainRequest, 240);

    Assert.assertNotNull(meta.getEvaluationMetrics().getRmse());
    Assert.assertNotNull(meta.getEvaluationMetrics().getR2());
    Assert.assertNotNull(meta.getEvaluationMetrics().getMae());
    Assert.assertNotNull(meta.getEvaluationMetrics().getEvariance());

    // now use the model to make some predictions

    List<Schema.Field> predictionFields = new ArrayList<>(schema.getFields());
    predictionFields.add(Schema.Field.of("predictedPrice", Schema.of(Schema.Type.DOUBLE)));
    Schema predictionSchema = Schema.recordOf("prediction", predictionFields);
    String testInputTable = "regressorTestInput";
    String outputTable = "regressorOutput";
    ETLPlugin predictor = new ETLPlugin("MLPredictor", SparkCompute.PLUGIN_TYPE,
                                        ImmutableMap.of("experimentId", experiment.getName(),
                                                        "modelId", meta.getId(),
                                                        "predictionField", "predictedPrice",
                                                        "schema", predictionSchema.toString()));
    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(testInputTable, schema)))
      .addStage(new ETLStage("predictor", predictor))
      .addStage(new ETLStage("sink", MockSink.getPlugin(outputTable)))
      .addConnection("source", "predictor")
      .addConnection("predictor", "sink")
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("regressionPredictor");
    ApplicationManager appManager = deployApplication(appId, new AppRequest<>(APP_ARTIFACT, config));

    // write some data to predict
    StructuredRecord listing1 = StructuredRecord.builder(schema)
      .set("city", "Palo Alto")
      .set("zip", "94306")
      .set("type", "Single-Family Home")
      .set("beds", "3")
      .set("baths", "3")
      .set("size", 2000d)
      .set("lot", 8000d)
      .set("stories", 1d)
      .build();
    StructuredRecord listing2 = StructuredRecord.builder(schema)
      .set("city", "Sunnyvale")
      .set("zip", "94086")
      .set("type", "Condo")
      .set("beds", "3")
      .set("baths", "3")
      .set("size", 2000d)
      .set("lot", 1000d)
      .set("stories", 3d)
      .build();
    // has values for categories that don't exist in the training data. Should get filtered.
    StructuredRecord listing3 = StructuredRecord.builder(schema)
      .set("city", "New York")
      .set("zip", "10001")
      .set("type", "Condo")
      .set("beds", "3")
      .set("baths", "3")
      .set("size", 2000d)
      .set("lot", 1000d)
      .set("stories", 3d)
      .build();
    DataSetManager<Table> testInputManager = getDataset(testInputTable);
    MockSource.writeInput(testInputManager, ImmutableList.of(listing1, listing2, listing3));

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    // check output
    DataSetManager<Table> outputManager = getDataset(outputTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Assert.assertEquals(2, outputRecords.size());
    double paloAltoPrice = 0;
    double sunnyvalePrice = 0;
    for (StructuredRecord outputRecord : outputRecords) {
      if ("Sunnyvale".equals(outputRecord.get("city"))) {
        sunnyvalePrice = outputRecord.get("predictedPrice");
      } else {
        paloAltoPrice = outputRecord.get("predictedPrice");
      }
    }
    Assert.assertNotEquals(0, paloAltoPrice);
    Assert.assertNotEquals(0, sunnyvalePrice);
    Assert.assertTrue(paloAltoPrice > sunnyvalePrice);
  }

  @Test
  public void testSplitter() throws Exception {
    Experiment experiment = new Experiment("exid", "some experiment", "dummypath", "price",
                                           Schema.Type.DOUBLE.name(), "workspace1");
    putExperiment(experiment);

    Schema schema = Schema.recordOf(
      "x",
      Schema.Field.of("int", Schema.nullableOf(Schema.of(Schema.Type.INT))),
      Schema.Field.of("long", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("float", Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
      Schema.Field.of("double", Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("string", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("bool", Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN))));

    DataSplit dataSplit = DataSplit.builder()
      .setType("random")
      .setSchema(schema)
      .build();
    DataSplitStats stats = splitWithPipeline(experiment.getName(), dataSplit, inputManager -> {
      List<StructuredRecord> input = new ArrayList<>();
      for (int i = 1; i <= 100; i++) {
        StructuredRecord.Builder builder = StructuredRecord.builder(schema);
        if (i % 10 > 0) {
          builder.set("int", i)
            .set("long", (long) i)
            .set("float", (float) i)
            .set("double", (double) i)
            .set("string", String.valueOf(i))
            .set("bool", i % 2 == 0)
            .build();
        }
        input.add(builder.build());
      }
      try {
        MockSource.writeInput(inputManager, input);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    Assert.assertNotNull(stats.getTrainingPath());
    Assert.assertNotNull(stats.getTestPath());
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      ColumnStats trainingStats = stats.getTrainingStats().get(fieldName);
      ColumnStats testStats = stats.getTestStats().get(fieldName);
      long totalNulls = trainingStats.getNullCount() + testStats.getNullCount();
      long totalCount = trainingStats.getTotalCount() + testStats.getTotalCount();
      Assert.assertEquals(100, totalCount);
      Assert.assertEquals(10, totalNulls);

      checkHistoCount(trainingStats);
      checkHistoCount(testStats);
    }
  }

  private DataSplitStats splitWithPipeline(String experiment, DataSplit split,
                                           Consumer<DataSetManager<Table>> inputWriter) throws Exception {
    DataSetManager<PartitionedFileSet> splitsDatasetManager = getDataset(Constants.Dataset.SPLITS);
    PartitionedFileSet splitsDataset = splitsDatasetManager.get();
    DataSplitTable dataSplitTable = new DataSplitTable(splitsDataset);
    String splitId = dataSplitTable.addSplit(experiment, split);
    splitsDatasetManager.flush();

    Map<String, String> properties = ImmutableMap.of("experimentId", experiment, "splitId", splitId);

    String inputName = UUID.randomUUID().toString();
    ETLBatchConfig config = ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(inputName, split.getSchema())))
      .addStage(new ETLStage("splitter", new ETLPlugin("DataSplitter", SparkSink.PLUGIN_TYPE, properties)))
      .addConnection("source", "splitter")
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app(UUID.randomUUID().toString());
    ApplicationManager appManager = deployApplication(appId, new AppRequest<>(APP_ARTIFACT, config));

    DataSetManager<Table> inputManager = getDataset(inputName);
    inputWriter.accept(inputManager);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    splitsDatasetManager.flush();
    return dataSplitTable.get(new SplitKey(experiment, splitId));
  }

  private void checkHistoCount(ColumnStats columnStats) {
    long histoCount = 0L;
    for (HistogramBin bin : columnStats.getHisto()) {
      histoCount += bin.getCount();
    }
    Assert.assertEquals(columnStats.getTotalCount() - columnStats.getNullCount(), histoCount);
  }

  private static void putExperiment(Experiment experiment) throws IOException {
    URL url = new URL(serviceURL + "/experiments/" + experiment.getName());
    HttpRequest request = HttpRequest.put(url)
      .withBody(GSON.toJson(experiment))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private static void deleteExperiment(String experiment) throws IOException {
    URL url = new URL(serviceURL + "/experiments/" + experiment);
    HttpResponse response = HttpRequests.execute(HttpRequest.delete(url).build());
    Assert.assertEquals(200, response.getResponseCode());
  }

  private static List<ExperimentStats> listExperiments() throws IOException {
    HttpResponse response = HttpRequests.execute(HttpRequest.get(new URL(serviceURL + "/experiments")).build());
    Assert.assertEquals(200, response.getResponseCode());
    return GSON.fromJson(response.getResponseBodyAsString(), new TypeToken<List<ExperimentStats>>() { }.getType());
  }

  private static DataSplitStats addSplit(String experiment, DataSplit split, int timeoutSeconds) throws Exception {
    URL url = new URL(serviceURL + "/experiments/" + experiment + "/splits");
    HttpRequest request = HttpRequest.post(url)
      .withBody(GSON.toJson(split))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    Id splitIdObj = GSON.fromJson(response.getResponseBodyAsString(), Id.class);
    String splitId = splitIdObj.getId();

    long start = System.currentTimeMillis();
    url = new URL(serviceURL + "/experiments/" + experiment + "/splits/" + splitId);
    request = HttpRequest.get(url).build();
    while (TimeUnit.SECONDS.convert(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS) <= timeoutSeconds) {
      response = HttpRequests.execute(request);
      DataSplitStats splitStats = GSON.fromJson(response.getResponseBodyAsString(), DataSplitStats.class);
      if (splitStats.getTrainingPath() != null) {
        return splitStats;
      }
      TimeUnit.SECONDS.sleep(timeoutSeconds / 10L);
    }
    throw new TimeoutException("Timed out waiting for split.");
  }

  private static String createModel(String experiment, CreateModelRequest createModelRequest) throws IOException {
    URL url = new URL(serviceURL + "/experiments/" + experiment + "/models");
    HttpRequest request = HttpRequest.post(url)
      .withBody(GSON.toJson(createModelRequest))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
    Id modelIdObj = GSON.fromJson(response.getResponseBodyAsString(), Id.class);
    return modelIdObj.getId();
  }

  private void assignSplit(String experiment, String modelId, String splitId) throws IOException {
    URL url = new URL(serviceURL + "/experiments/" + experiment + "/models/" + modelId + "/split");
    HttpRequest request = HttpRequest.put(url)
      .withBody(GSON.toJson(new Id(splitId)))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private static ModelMeta trainModel(String experiment, String modelId, TrainModelRequest trainRequest,
                                      int timeoutSeconds) throws Exception {
    URL url = new URL(serviceURL + "/experiments/" + experiment + "/models/" + modelId + "/train");
    HttpRequest request = HttpRequest.post(url)
      .withBody(GSON.toJson(trainRequest))
      .build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());

    long start = System.currentTimeMillis();
    url = new URL(serviceURL + "/experiments/" + experiment + "/models/" + modelId);
    request = HttpRequest.get(url).build();
    while (TimeUnit.SECONDS.convert(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS) <= timeoutSeconds) {
      response = HttpRequests.execute(request);
      ModelMeta modelMeta = GSON.fromJson(response.getResponseBodyAsString(), ModelMeta.class);
      if (modelMeta.getStatus() == ModelStatus.TRAINING_FAILED) {
        throw new Exception("Model failed to train.");
      } else if (modelMeta.getStatus() == ModelStatus.TRAINED) {
        return modelMeta;
      }
      TimeUnit.SECONDS.sleep(timeoutSeconds / 10L);
    }
    throw new TimeoutException("Timed out waiting for model.");
  }
}
