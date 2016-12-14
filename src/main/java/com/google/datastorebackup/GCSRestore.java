package com.google.datastorebackup;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.io.datastore.DatastoreV1;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.datastore.DatastoreIO;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Entity.Builder;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;


public class GCSRestore {
  public static void run(String[] args) {
    System.out.println("Making GCS->Datastore pipeline");

    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Options.class);

    if (options.getIsBlocking()) {
      options.setRunner(BlockingDataflowPipelineRunner.class);
    } else {
      options.setRunner(DataflowPipelineRunner.class);
      options.setStreaming(false);
    }

    // Create Datastore sink
    DatastoreV1.Write write = DatastoreIO.v1().write()
        .withProjectId(options.getProject());


    // Build our data pipeline
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply("ReadBackup", TextIO.Read.from(options.getBackupGCSPrefix() + "*"))
    .apply("JsonToEntity", ParDo.of(new JsonToEntity()))
    .apply("EntityToDatastore", write);

    System.out.println("Running pipeline");
    pipeline.run();
  }

  private interface Options extends DataflowPipelineOptions {
    @Description("Datastore entity kind")
    @Validation.Required
    String getDatastoreEntityKind();
    void setDatastoreEntityKind(String datastoreEntityKind);

    @Description("GCS Datastore Backup Prefix")
    @Validation.Required
    String getBackupGCSPrefix();
    void setBackupGCSPrefix(String outputGCSPrefix);

    @Description("Block until dataflow job finishes")
    boolean getIsBlocking();
    void setIsBlocking(boolean isBlocking);
  }

  static class JsonToEntity extends DoFn<String, Entity> {
    private static JsonFormat.Parser jsonParser = createParser();
    
    public static JsonFormat.Parser createParser() {
      TypeRegistry typeRegistry = TypeRegistry.newBuilder()
        .add(Entity.getDescriptor())
        .build();

      return JsonFormat.parser()
          .usingTypeRegistry(typeRegistry);
    }

    @Override
    public void processElement(DoFn<String, Entity>.ProcessContext context) throws Exception {
      String json = context.element();
      Builder builder = Entity.newBuilder();
      jsonParser.merge(json, builder);
      context.output(builder.build());
    }
  }
}
