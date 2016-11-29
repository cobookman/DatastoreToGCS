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
import com.google.datastore.v1.Query;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


public class Backup {
  public static void run(String[] args) {
    System.out.println("Making Datastore->GCS pipeline");
    
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Options.class);
    
    if (options.getIsBlocking()) {
      options.setRunner(BlockingDataflowPipelineRunner.class);
    } else {
      options.setRunner(DataflowPipelineRunner.class);
      options.setStreaming(false);
    }

    
    // Add Timestamp + Entity Kind to backup files
    DateFormat dateFormat = new SimpleDateFormat("yyyy.MM.dd_HH.mm.ss");
    String outputLocation = String.format("%s%s.%s", options.getBackupGCSPrefix(),
        options.getDatastoreEntityKind() ,
        dateFormat.format(new Date()));
    
    if (options.getOutputLocation().endsWith("/")) {
      outputLocation += "/";
    } else {
      outputLocation = "." + outputLocation;
    }
    
    // Build our Datastore query.
    // Right now we are simply grabbing all Datastore records of a given kind,
    Query.Builder queryBuilder = Query.newBuilder();
    queryBuilder.addKindBuilder().setName(options.getDatastoreEntityKind());
    Query query = queryBuilder.build();
    
    // Generate the Datastore Read Source
    DatastoreV1.Read read = DatastoreIO.v1().read()
        .withProjectId(options.getProject())
        .withQuery(query);
    
    // Build our data pipeline
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply("IngestEntities", read)
            .apply("EntityToJson", ParDo.of(new DatastoreToJson()))
            .apply("WriteJson", TextIO.Write.to(outputLocation)
                .withSuffix(".json"));
    
    System.out.println("Running pipeline");
    pipeline.run();
  }

  private interface Options extends DataflowPipelineOptions {
    @Description("Datastore entity kind")
    @Validation.Required
    String getDatastoreEntityKind();
    void setDatastoreEntityKind(String datastoreEntityKind);

    @Description("Output GCS Filename Prefix")
    @Validation.Required
    String getBackupGCSPrefix();
    void setBackupGCSPrefix(String backupGCSPrefix);
    
    @Description("Block until dataflow job finishes")
    boolean getIsBlocking();
    void setIsBlocking(boolean isBlocking);
  }

  static class DatastoreToJson extends DoFn<Entity, String> {
    private static JsonFormat.Printer jsonPrinter = createPrinter();
    
    public static JsonFormat.Printer createPrinter() {
      TypeRegistry typeRegistry = TypeRegistry.newBuilder()
        .add(Entity.getDescriptor())
        .build();
      
      return JsonFormat.printer()
          .usingTypeRegistry(typeRegistry)
          .omittingInsignificantWhitespace();
    }
    
    @Override
    public void processElement(DoFn<Entity, String>.ProcessContext context) throws Exception {
      Entity elm = context.element();
      String output = jsonPrinter.print(elm);
      context.output(output);
    }
  }
}
