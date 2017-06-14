package com.google.cloud.dataflow.teleport;

import com.google.cloud.dataflow.teleport.Helpers.JSTransform;

import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import javax.script.Invocable;
import javax.script.ScriptException;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import com.google.datastore.v1.Entity;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exports Datastore Entities to GCS as newline deliminted Protobuf v3 Json.
 */
public class DatastoreToGcs {

  private static final Logger mLogger = LoggerFactory.getLogger(DatastoreToGcs.class);

  /**
   * Runs the DatastoreToGcs dataflow pipeline
   */
  public static void main(String[] args) throws IOException, ScriptException {
    Options options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(Options.class);

    options.setRunner(DataflowRunner.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("IngestEntities",
            DatastoreIO.v1().read()
                .withProjectId(options.getProject())
                .withLiteralGqlQuery(options.getGqlQuery())
                .withNamespace(options.getNamespace()))
        .apply("EntityToJson", ParDo.of(new EntityToJson(options)))
        .apply("JsonToGcs", TextIO.write().to(options.getGcsSavePath())
            .withSuffix(".json"));

    pipeline.run();
  }

  interface Options extends GcpOptions {

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

    @Description("GCS path to javascript fn for transforming output")
    ValueProvider<String> getGcsJsTransformFns();
    void setGcsJsTransformFns(ValueProvider<String> gcsJsTransformFns);
  }

  /**
   * Converts a Datstore Entity to Protobuf encoded Json
   */
  static class EntityToJson extends DoFn<Entity, String> {
    protected JsonFormat.Printer mJsonPrinter;
    protected ValueProvider<String> mTransformValueProvider;
    protected Invocable mInvocable;

    public EntityToJson(Options options) {
      mTransformValueProvider = options.getGcsJsTransformFns();
    }

    private JsonFormat.Printer getJsonPrinter() {
      if (mJsonPrinter == null) {
        TypeRegistry typeRegistry = TypeRegistry.newBuilder()
            .add(Entity.getDescriptor())
            .build();

        mJsonPrinter = JsonFormat.printer()
            .usingTypeRegistry(typeRegistry)
            .omittingInsignificantWhitespace();
      }
      return mJsonPrinter;
    }

    @Nullable
    private Invocable getInvocable() throws ScriptException {
      if (mInvocable == null) {
        return JSTransform.buildInvocable(mTransformValueProvider.get());
      }
      return mInvocable;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Entity entity = c.element();
      String json = getJsonPrinter().print(entity);

      if (getInvocable() != null) {
        json = (String) getInvocable().invokeFunction("transform", json);
      }
      c.output(json);
    }
  }

}
