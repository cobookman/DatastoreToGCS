package com.google.cloud.dataflow.teleport;

import com.google.cloud.dataflow.teleport.Helpers.JSTransform;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Entity.Builder;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import java.io.IOException;
import javax.annotation.Nullable;
import javax.script.Invocable;
import javax.script.ScriptException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GCS to Datastore Records
 */
public class GcsToDatastore {
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
        .apply("IngestJson", TextIO.read()
            .from(options.getGcsPathPrefix()))
        .apply("GcsToEntity", ParDo.of(new JsonToEntity(options)))
        .apply(DatastoreIO.v1().write()
            .withProjectId(options.getDatastoreProject()));

    pipeline.run();
  }

  interface Options extends GcpOptions {
    @Validation.Required
    @Description("GCS Data Path E.g: gs://mybucket/somepath/")
    ValueProvider<String> getGcsPathPrefix();
    void setGcsPathPrefix(ValueProvider<String> gcsPathPrefix);

    @Description("GCS path to javascript fn for transforming output")
    ValueProvider<String> getGcsJsTransformFns();
    void setGcsJsTransformFns(ValueProvider<String> gcsJsTransformFns);

    ValueProvider<String> getDatastoreProject();
    void setDatastoreProject(ValueProvider<String> datastoreProject);
  }

  /**
   * Converts a Protobuf Encoded Json String to a Datastore Entity
   */
  static class JsonToEntity extends DoFn<String, Entity> {
    protected JsonFormat.Parser mJsonParser;
    protected ValueProvider<String> mTransformValueProvider;
    protected Invocable mInvocable;

    public JsonToEntity(Options options) {
      mTransformValueProvider = options.getGcsJsTransformFns();
    }

    private JsonFormat.Parser getJsonParser() {
      if (mJsonParser == null) {
        TypeRegistry typeRegistry = TypeRegistry.newBuilder()
            .add(Entity.getDescriptor())
            .build();

        mJsonParser = JsonFormat.parser()
            .usingTypeRegistry(typeRegistry);
      }
      return mJsonParser;
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
      String entityJson = c.element();

      if (getInvocable() != null) {
        entityJson = (String) getInvocable().invokeFunction("transform", entityJson);
      }

      Builder builder = Entity.newBuilder();
      getJsonParser().merge(entityJson, builder);

      c.output(builder.build());
    }
  }
}
