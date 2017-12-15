package co.cask.mmds.manager.runner;

import co.cask.cdap.api.artifact.ArtifactScope;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.client.ProgramClient;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.etl.proto.ArtifactSelectorConfig;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.mmds.data.DataSplitInfo;
import com.google.common.base.Joiner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Trains a model by running a pipeline. Temporary.
 */
public class PipelineExecutor {
  private final ApplicationId splitterAppId;
  private final ProgramId splitterProgramId;
  private static final Schema FILE_SCHEMA =
    Schema.recordOf("text",
                    Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
                    Schema.Field.of("body", Schema.of(Schema.Type.STRING)));

  public PipelineExecutor(String namespace) {
    this.splitterAppId = new NamespaceId(namespace).app("MMDS-Splitter");
    this.splitterProgramId = splitterAppId.program(ProgramType.WORKFLOW, "DataPipelineWorkflow");
  }

  public void split(DataSplitInfo dataSplitInfo) throws Exception {
    ClientConfig clientConfig = ClientConfig.builder()
      .setConnectionConfig(new ConnectionConfig("localhost", 11015, false))
      .build();
    ApplicationClient appClient = new ApplicationClient(clientConfig);
    if (!appClient.exists(splitterAppId)) {
      deploySplitterPipeline(appClient);
    }

    ProgramClient programClient = new ProgramClient(clientConfig);
    Map<String, String> args = new HashMap<>();
    args.put("srcpath", dataSplitInfo.getExperiment().getSrcpath());
    args.put("schema", dataSplitInfo.getDataSplit().getSchema().toString());
    args.put("directives", Joiner.on("\n").join(dataSplitInfo.getDataSplit().getDirectives()));
    args.put("experimentId", dataSplitInfo.getExperiment().getName());
    args.put("splitId", dataSplitInfo.getSplitId());
    args.put("outcome", dataSplitInfo.getExperiment().getOutcome());

    programClient.start(splitterProgramId, false, args);
  }

  private void deploySplitterPipeline(ApplicationClient appClient) throws IOException, UnauthenticatedException {
    Map<String, String> splitterArgs = new HashMap<>();
    splitterArgs.put("experimentId", "${experimentId}");
    splitterArgs.put("splitId", "${splitId}");
    splitterArgs.put("schema", "${schema}");
    ArtifactSelectorConfig splitterArtifact =
      new ArtifactSelectorConfig("SYSTEM", "mmds-plugins", "[1.0.0-SNAPSHOT,1.1.0)");
    ETLStage sink = new ETLStage("splitter",
                                 new ETLPlugin("DataSplitter", "sparksink", splitterArgs, splitterArtifact));
    ETLBatchConfig conf = createConfig(sink);

    AppRequest<ETLBatchConfig> request =
      new AppRequest<>(new ArtifactSummary("cdap-data-pipeline", "5.0.0-SNAPSHOT", ArtifactScope.SYSTEM), conf);
    appClient.deploy(splitterAppId, request);
  }

  private ETLBatchConfig createConfig(ETLStage sink) {
    Map<String, String> fileArgs = new HashMap<>();
    fileArgs.put("referenceName", "ModelTrainerInput");
    fileArgs.put("format", "text");
    fileArgs.put("path", "${srcpath}");
    fileArgs.put("schema", FILE_SCHEMA.toString());
    ArtifactSelectorConfig fileArtifact =
      new ArtifactSelectorConfig("SYSTEM", "core-plugins", "[2.0.0-SNAPSHOT,2.1.0)");

    Map<String, String> wranglerArgs = new HashMap<>();
    wranglerArgs.put("field", "*");
    wranglerArgs.put("threshold", "100000");
    wranglerArgs.put("schema", "${schema}");
    wranglerArgs.put("directives", "${directives}");
    wranglerArgs.put("precondition", "false");
    ArtifactSelectorConfig wrangerArtifact =
      new ArtifactSelectorConfig("SYSTEM", "wrangler-transform", "[3.0.0-SNAPSHOT,3.2.0)");

    return ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("file", new ETLPlugin("File", "batchsource", fileArgs, fileArtifact)))
      .addStage(new ETLStage("wrangler", new ETLPlugin("Wrangler", "transform", wranglerArgs, wrangerArtifact)))
      .addStage(sink)
      .addConnection("file", "wrangler")
      .addConnection("wrangler", sink.getName())
      .build();
  }

}
