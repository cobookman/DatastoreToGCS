package com.google.cloud.dataflow.teleport;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;

import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;

import com.google.datastore.v1.Entity;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.io.IOException;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Exports Datastore Entities to GCS as newline deliminted Protobuf v3 Json.
 */
public class DatastoreToGcs {

  /**
   * Runs the DatastoreToGcs dataflow pipeline
   * @param args
   */
  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Options.class);

//    System.out.println("Pipeline runner of: " + options.getRunner().toString());

    // Build DatastoreToGCS pipeline
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("IngestEntities",
            DatastoreIO.v1().read()
              .withProjectId(options.getProjectId())
              .withLiteralGqlQuery(options.getGqlQuery())
              .withNamespace(options.getNamespace()))
        .apply("EntityToJson", ParDo.of(new EntityToJson()))
        .apply("JsonToGcs", TextIO.write().to(options.getGcsSavePath())
            .withSuffix(".json"));

    // Start the job
    PipelineResult pipelineResult = pipeline.run();

    if (options.getKeepJobsRunning()) {
      System.out.println("Blocking until done");
      try {
        System.out.println(pipelineResult.waitUntilFinish());
      } catch (Exception exc) {
        System.err.println(exc);
        pipelineResult.cancel();
      }
    }
  }

   interface Options extends CommonOptions {
    @Validation.Required
    @Description("GCS Path E.g: gs://mybucket/somepath/")
    ValueProvider<String> getGcsSavePath();
    void setGcsSavePath(ValueProvider<String> gcsSavePath);

    @Validation.Required
    @Description("GQL Query to get the datastore Entities")
    ValueProvider<String> getGqlQuery();
    void setGqlQuery(ValueProvider<String> gqlQuery);

    @Validation.Required
    @Description("Namespace of Entities, use `\"\"` for default")
    ValueProvider<String> getNamespace();
    void setNamespace(ValueProvider<String> namespace);
  }

  static class EntityToJson extends DoFn<Entity, String> {
    protected static JsonFormat.Printer mJsonPrinter = null;

    public static JsonFormat.Printer getPrinter() {
      if (mJsonPrinter == null) {
        TypeRegistry typeRegistry = TypeRegistry.newBuilder()
            .add(Entity.getDescriptor())
            .build();

        mJsonPrinter = JsonFormat.printer()
            .usingTypeRegistry(typeRegistry)
            .includingDefaultValueFields()
            .omittingInsignificantWhitespace();
      }
      return mJsonPrinter;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Entity entity = c.element();
      c.output(getPrinter().print(entity));
    }
  }
}
