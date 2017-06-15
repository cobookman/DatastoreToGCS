/*
  Copyright 2017 Google Inc.
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.google.cloud.dataflow.teleport;

import com.google.cloud.dataflow.teleport.Helpers.JSTransform;

import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.TypeRegistry;
import javax.script.ScriptException;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
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
                .withProjectId(options.getDatastoreProject())
                .withLiteralGqlQuery(options.getGqlQuery())
                .withNamespace(options.getNamespace()))
        .apply("EntityToJson", ParDo.of(new EntityToJson(options.getJsTransformPath())))
        .apply("JsonToGcs", TextIO.write().to(options.getSavePath())
            .withSuffix(".json"));

    pipeline.run();
  }

  interface Options extends PipelineOptions {

    @Validation.Required
    @Description("GCS Path E.g: gs://mybucket/somepath/")
    ValueProvider<String> getSavePath();
    void setSavePath(ValueProvider<String> savePath);

    @Validation.Required
    @Description("GQL Query to specify which datastore Entities")
    ValueProvider<String> getGqlQuery();
    void setGqlQuery(ValueProvider<String> gqlQuery);

    @Description("Project to grab Datastore Entities from")
    ValueProvider<String> getDatastoreProject();
    void setDatastoreProject(ValueProvider<String> datastoreProject);

    @Validation.Required
    @Description("Namespace of requested Entities, use `\"\"` for default")
    ValueProvider<String> getNamespace();
    void setNamespace(ValueProvider<String> namespace);

    @Description("GCS path to javascript fn for transforming output")
    ValueProvider<String> getJsTransformPath();
    void setJsTransformPath(ValueProvider<String> jsTransformPath);
  }

  /**
   * Converts a Datstore Entity to Protobuf encoded Json
   */
  public static class EntityToJson extends DoFn<Entity, String> {
    protected JsonFormat.Printer mJsonPrinter;
    protected JSTransform mJSTransform;
    protected ValueProvider<String> mJsTransformPath;

    public EntityToJson(ValueProvider<String> jsTransformPath) {
      mJsTransformPath = jsTransformPath;
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

    private JSTransform getJSTransform() throws ScriptException {
      if (mJSTransform == null) {
        String jsTransformPath = "";
        if (mJsTransformPath.isAccessible()) {
          jsTransformPath = mJsTransformPath.get();
        }

        mJSTransform = JSTransform.newBuilder()
            .setGcsJSPath(jsTransformPath)
            .build();
      }
      return mJSTransform;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      Entity entity = c.element();
      String json = getJsonPrinter().print(entity);

      if (getJSTransform().hasTransform()) {
        json = getJSTransform().invoke(json);
      }

      c.output(json);
    }
  }

}
