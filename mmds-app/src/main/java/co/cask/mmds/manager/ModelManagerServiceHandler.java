/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.mmds.manager;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.api.spark.service.SparkHttpServiceContext;
import co.cask.cdap.api.spark.service.SparkHttpServiceHandler;
import co.cask.cdap.api.spark.sql.DataFrames;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.mmds.ModelLogging;
import co.cask.mmds.api.Modeler;
import co.cask.mmds.data.DataSplit;
import co.cask.mmds.data.DataSplitInfo;
import co.cask.mmds.data.DataSplitStats;
import co.cask.mmds.data.DataSplitTable;
import co.cask.mmds.data.Experiment;
import co.cask.mmds.data.ExperimentMetaTable;
import co.cask.mmds.data.ExperimentStore;
import co.cask.mmds.data.ModelKey;
import co.cask.mmds.data.ModelMeta;
import co.cask.mmds.data.ModelTable;
import co.cask.mmds.data.ModelTrainerInfo;
import co.cask.mmds.data.SplitKey;
import co.cask.mmds.manager.runner.AlgorithmSpec;
import co.cask.mmds.manager.runner.PipelineExecutor;
import co.cask.mmds.modeler.Modelers;
import co.cask.mmds.modeler.param.spec.ParamSpec;
import co.cask.mmds.modeler.train.ModelOutput;
import co.cask.mmds.modeler.train.ModelOutputWriter;
import co.cask.mmds.modeler.train.ModelTrainer;
import co.cask.mmds.proto.BadRequestException;
import co.cask.mmds.proto.CreateModelRequest;
import co.cask.mmds.proto.EndpointException;
import co.cask.mmds.proto.TrainModelRequest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * Model Service handler
 */
public class ModelManagerServiceHandler implements SparkHttpServiceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ModelManagerServiceHandler.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .serializeSpecialFloatingPointValues()
    .create();
  private String modelMetaDataset;
  private String modelComponentsDataset;
  private String experimentMetaDataset;
  private String splitsDataset;
  private ModelOutputWriter modelOutputWriter;
  private SparkSession sparkSession;
  private SparkHttpServiceContext context;
  private PipelineExecutor pipelineExecutor;

  @Override
  public void initialize(SparkHttpServiceContext context) throws Exception {
    this.context = context;
    sparkSession = context.getSparkSession();
    Map<String, String> properties = context.getSpecification().getProperties();
    modelMetaDataset = properties.get("modelMetaDataset");
    modelComponentsDataset = properties.get("modelComponentsDataset");
    experimentMetaDataset = properties.get("experimentMetaDataset");
    splitsDataset = properties.get("splitsDataset");
    pipelineExecutor = new PipelineExecutor(context.getNamespace());
    context.execute(datasetContext -> {
      FileSet modelComponents = datasetContext.getDataset(modelComponentsDataset);
      modelOutputWriter = new ModelOutputWriter(context.getAdmin(), context,
                                                modelComponents.getBaseLocation(), true);
    });
  }

  @Override
  public void destroy() {
    // no-op
  }

  @GET
  @Path("/algorithms")
  public void listAlgorithms(HttpServiceRequest request, HttpServiceResponder responder) {
    List<AlgorithmSpec> algorithms = new ArrayList<>();
    for (String algorithm : Modelers.getAlgorithms()) {
      Modeler modeler = Modelers.getModeler(algorithm);
      List<ParamSpec> paramSpecs = modeler.getParams(new HashMap<>()).getSpec();
      algorithms.add(new AlgorithmSpec(algorithm, paramSpecs));
    }
    responder.sendString(GSON.toJson(algorithms));
  }

  @GET
  @Path("/algorithms/{algorithm}")
  public void getAlgorithm(HttpServiceRequest request, HttpServiceResponder responder,
                           @PathParam("algorithm") String algorithm) {
    Modeler modeler = Modelers.getModeler(algorithm);
    if (modeler == null) {
      responder.sendError(404, "Algorithm " + algorithm + " not found.");
      return;
    }
    List<ParamSpec> paramSpecs = modeler.getParams(new HashMap<>()).getSpec();
    responder.sendString(GSON.toJson(new AlgorithmSpec(algorithm, paramSpecs)));
  }

  /**
   * Get List of {@link Experiment}s.
   *
   * @param request http request
   * @param responder http response containing list of experiments as json string
   */
  @GET
  @Path("/experiments")
  public void listExperiments(HttpServiceRequest request, HttpServiceResponder responder,
                              final @QueryParam("offset") @DefaultValue("0") int offset,
                              final @QueryParam("limit") @DefaultValue("20") int limit,
                              final @QueryParam("srcPath") @DefaultValue("") String srcPath) {
    runInTx(responder, store -> {
      validate(offset, limit);
      responder.sendString(GSON.toJson(store.listExperiments(offset, limit, e -> e.getSrcpath().equals(srcPath))));
    });
  }

  private void validate(int offset, int limit) {
    if (offset < 0) {
      throw new BadRequestException("Offset must be zero or a positive number");
    }

    if (limit <= 0) {
      throw new BadRequestException("Limit must be a positive number");
    }
  }

  @GET
  @Path("/experiments/{experiment-name}")
  public void getExperiment(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("experiment-name") final String experimentName) {
    runInTx(responder, store -> responder.sendString(GSON.toJson(store.getExperimentStats(experimentName))));
  }

  @PUT
  @Path("/experiments/{experiment-name}")
  public void putExperiment(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("experiment-name") String experimentName) {
    runInTx(responder, store -> {
      try {
        Experiment experiment = GSON.fromJson(Bytes.toString(request.getContent()), Experiment.class);
        experiment.validate();
        Experiment experimentInfo = new Experiment(experimentName, experiment);
        store.putExperiment(experimentInfo);
        responder.sendStatus(200);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(e.getMessage());
      } catch (JsonSyntaxException e) {
        throw new BadRequestException(
          String.format("Problem occurred while parsing request body for Experiment: %s. " +
                          "Please provide valid json. Error: %s", experimentName, e.getMessage()));
      }
    });
  }

  @DELETE
  @Path("/experiments/{experiment-name}")
  public void deleteExperiment(HttpServiceRequest request, HttpServiceResponder responder,
                               @PathParam("experiment-name") final String experimentName) {
    runInTx(responder, store -> {
      store.deleteExperiment(experimentName);
      responder.sendStatus(200);
    });
  }

  @GET
  @Path("/experiments/{experiment-name}/models")
  public void listModels(HttpServiceRequest request, HttpServiceResponder responder,
                         final @PathParam("experiment-name") String experimentName,
                         final @QueryParam("offset") @DefaultValue("0") int offset,
                         final @QueryParam("limit") @DefaultValue("20") int limit) {
    runInTx(responder, store -> {
      validate(offset, limit);
      responder.sendString(GSON.toJson(store.listModels(experimentName, offset, limit)));
    });
  }

  @GET
  @Path("/experiments/{experiment-name}/models/{model-id}")
  public void getModel(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("experiment-name") String experimentName,
                       @PathParam("model-id") String modelId) throws Exception {
    final ModelKey modelKey = new ModelKey(experimentName, modelId);
    runInTx(responder, store -> responder.sendString(GSON.toJson(store.getModel(modelKey))));
  }

  @GET
  @Path("/experiments/{experiment-name}/models/{model-id}/status")
  public void getModelStatus(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("experiment-name") String experimentName,
                             @PathParam("model-id") String modelId) throws Exception {
    final ModelKey modelKey = new ModelKey(experimentName, modelId);
    runInTx(responder, store -> {
      ModelMeta meta = store.getModel(modelKey);
      responder.sendString(meta.getStatus().name());
    });
  }

  @POST
  @Path("/experiments/{experiment-name}/models")
  public void addModel(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("experiment-name") final String experimentName) throws Exception {
    runInTx(responder, store -> {
      CreateModelRequest createRequest = GSON.fromJson(Bytes.toString(request.getContent()), CreateModelRequest.class);
      createRequest.validate();
      String modelId = store.addModel(experimentName, createRequest);
      responder.sendString(GSON.toJson(new Id(modelId)));
    });
  }

  @PUT
  @Path("/experiments/{experiment-name}/models/{model-id}/split")
  public void setModelSplit(HttpServiceRequest request, HttpServiceResponder responder,
                            @PathParam("experiment-name") final String experimentName,
                            @PathParam("model-id") final String modelId) throws Exception {
    runInTx(responder, store -> {
      Id splitId = GSON.fromJson(Bytes.toString(request.getContent()), Id.class);
      store.setModelSplit(new ModelKey(experimentName, modelId), splitId.getId());
      responder.sendStatus(200);
    });
  }

  @POST
  @Path("/experiments/{experiment-name}/models/{model-id}/train")
  public void trainModel(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("experiment-name") final String experimentName,
                         @PathParam("model-id") final String modelId) throws Exception {
    ModelTrainerInfo trainerInfo = callInTx(responder, store -> {
      TrainModelRequest trainRequest = GSON.fromJson(Bytes.toString(request.getContent()), TrainModelRequest.class);
      trainRequest.validate();

      ModelKey modelKey = new ModelKey(experimentName, modelId);

      return store.trainModel(modelKey, trainRequest);
    });

    // happens if there was an error above
    if (trainerInfo == null) {
      return;
    }

    ModelKey modelKey = new ModelKey(trainerInfo.getExperiment().getName(), trainerInfo.getModelId());

    new Thread(() -> {
      ModelLogging.start(modelKey.getExperiment(), modelKey.getModel());
      Schema schema = trainerInfo.getDataSplitStats().getSchema();
      ModelTrainer modelTrainer = new ModelTrainer(trainerInfo);
      StructType sparkSchema = DataFrames.toDataType(schema);
      try {
        // read training data
        Dataset<Row> rawTraining = sparkSession.read()
          .format("parquet")
          .schema(sparkSchema)
          .load(trainerInfo.getDataSplitStats().getTrainingPath());

        // read test data
        Dataset<Row> rawTest = sparkSession.read()
          .format("parquet")
          .schema(sparkSchema)
          .load(trainerInfo.getDataSplitStats().getTestPath());

        ModelOutput modelOutput = modelTrainer.train(rawTraining, rawTest);

        // write model components
        modelOutputWriter.save(modelKey, modelOutput, trainerInfo.getModel().getPredictionsDataset());

        // write model metadata
        runInTx(store -> store.updateModelMetrics(modelKey, modelOutput.getEvaluationMetrics(),
                                                  System.currentTimeMillis(), modelOutput.getFeatureNames(),
                                                  modelOutput.getCategoricalFeatures()));
      } catch (Exception e) {
        LOG.error("Error training model {} in experiment {}.", modelKey.getModel(), modelKey.getExperiment(), e);
        try {
          runInTx(store -> store.modelFailed(modelKey));
        } catch (TransactionFailureException te) {
          LOG.error("Error marking model {} in experiment {} as failed",
                    modelKey.getModel(), modelKey.getExperiment(), te);
        }

        try {
          modelOutputWriter.deleteComponents(modelKey);
        } catch (IOException e1) {
          LOG.error("Error during cleanup after model {} in experiment {} failed to train.",
                    modelKey.getModel(), modelKey.getExperiment(), e1);
        }
      } finally {
        ModelLogging.finish();
      }
    }).start();

    responder.sendString(GSON.toJson(new Id(trainerInfo.getModelId())));
  }

  @DELETE
  @Path("/experiments/{experiment-name}/models/{model-id}")
  public void deleteModel(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("experiment-name") String experimentName,
                          @PathParam("model-id") String modelId) {
    final ModelKey modelKey = new ModelKey(experimentName, modelId);
    runInTx(responder, store -> {
      store.deleteModel(modelKey);
      responder.sendStatus(200);
    });
  }

  @POST
  @Path("/experiments/{experiment-name}/models/{model-id}/deploy")
  public void deployModel(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("experiment-name") String experimentName,
                          @PathParam("model-id") String modelId) {
    final ModelKey key = new ModelKey(experimentName, modelId);
    runInTx(responder, store -> {
      store.deployModel(key);
      responder.sendStatus(200);
    });
  }

  @GET
  @Path("/experiments/{experiment-name}/splits")
  public void listSplits(HttpServiceRequest request, HttpServiceResponder responder,
                         @PathParam("experiment-name") final String experimentName) {
    runInTx(responder, store -> responder.sendString(GSON.toJson(store.listSplits(experimentName))));
  }

  @POST
  @Path("/experiments/{experiment-name}/splits")
  public void addSplit(final HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("experiment-name") final String experimentName) throws Exception {
    DataSplitInfo dataSplitInfo = callInTx(responder, store -> {
      try {
        DataSplit splitInfo = GSON.fromJson(Bytes.toString(request.getContent()), DataSplit.class);
        splitInfo.validate();
        return store.addSplit(experimentName, splitInfo);
      } catch (JsonParseException e) {
        throw new BadRequestException(
          String.format("Problem occurred while parsing request for model creation for experiment '%s'. " +
                          "Error: %s", experimentName, e.getMessage()));
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(e.getMessage());
      }
    });

    // happens if there was an error above
    if (dataSplitInfo == null) {
      return;
    }
    pipelineExecutor.split(dataSplitInfo);
    responder.sendString(GSON.toJson(new Id(dataSplitInfo.getSplitId())));
  }

  @GET
  @Path("/experiments/{experiment-name}/splits/{split-id}")
  public void getSplit(HttpServiceRequest request, HttpServiceResponder responder,
                       @PathParam("experiment-name") String experimentName,
                       @PathParam("split-id") String splitId) {
    final SplitKey key = new SplitKey(experimentName, splitId);
    runInTx(responder, store -> responder.sendString(GSON.toJson(store.getSplit(key))));
  }

  @GET
  @Path("/experiments/{experiment-name}/splits/{split-id}/status")
  public void getSplitStatus(HttpServiceRequest request, HttpServiceResponder responder,
                             @PathParam("experiment-name") String experimentName,
                             @PathParam("split-id") String splitId) {
    final SplitKey key = new SplitKey(experimentName, splitId);
    runInTx(responder, store -> {
      DataSplitStats stats = store.getSplit(key);
      responder.sendString(GSON.toJson(stats.getStatus()));
    });
  }

  @DELETE
  @Path("/experiments/{experiment-name}/splits/{split-id}")
  public void deleteSplit(HttpServiceRequest request, HttpServiceResponder responder,
                          @PathParam("experiment-name") String experimentName,
                          @PathParam("split-id") String splitId) {
    final SplitKey key = new SplitKey(experimentName, splitId);
    runInTx(responder, store -> {
      store.deleteSplit(key);
      responder.sendStatus(200);
    });

  }

  /**
   * Run some logic in a transaction, catching certain exceptions and responding with the relevant error code.
   * Any EndpointException thrown by the consumer will be handled automatically.
   */
  private void runInTx(final Consumer<ExperimentStore> consumer) throws TransactionFailureException {
    context.execute((datasetContext) -> {
      Table modelMeta = datasetContext.getDataset(modelMetaDataset);
      Table experiments = datasetContext.getDataset(experimentMetaDataset);
      PartitionedFileSet splits = datasetContext.getDataset(splitsDataset);
      ExperimentStore store = new ExperimentStore(
        new ExperimentMetaTable(experiments),
        new DataSplitTable(splits),
        new ModelTable(modelMeta));
      consumer.accept(store);
    });
  }

  /**
   * Run some logic in a transaction, catching certain exceptions and responding with the relevant error code.
   * Any EndpointException thrown by the consumer will be handled automatically.
   */
  private void runInTx(final HttpServiceResponder responder, final Consumer<ExperimentStore> consumer) {
    try {
      context.execute((datasetContext) -> {
          Table modelMeta = datasetContext.getDataset(modelMetaDataset);
          Table experiments = datasetContext.getDataset(experimentMetaDataset);
          PartitionedFileSet splits = datasetContext.getDataset(splitsDataset);
          ExperimentStore store = new ExperimentStore(
            new ExperimentMetaTable(experiments),
            new DataSplitTable(splits),
            new ModelTable(modelMeta));
          try {
            consumer.accept(store);
          } catch (EndpointException e) {
            responder.sendError(e.getCode(), e.getMessage());
          }
        });
    } catch (TransactionFailureException e) {
      LOG.error("Transaction failure during service call", e);
      responder.sendError(500, e.getMessage());
    }
  }

  /**
   * Run an endpoint method in a transaction, catching certain exceptions and responding with the relevant error code.
   * Any EndpointException thrown by the function will be handled automatically.
   */
  private <T> T callInTx(HttpServiceResponder responder, final Function<ExperimentStore, T> function) {
    AtomicReference<T> ref = new AtomicReference<>();
    runInTx(responder, store -> ref.set(function.apply(store)));
    return ref.get();
  }
}
